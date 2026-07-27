// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mempool

import (
	"container/heap"
	"slices"
)

// dagNode stores only graph/index metadata. Transaction bodies remain owned by
// Mempool so both implementations preserve the same immutable snapshot and
// relay behavior.
type dagNode struct {
	hash     string
	sequence uint64
	parents  map[string]struct{}
	children map[string]struct{}
	consumed []string
	produced []string
}

// transactionDAG indexes producer/consumer relationships between pending
// transactions. All methods are called under Mempool's mutation and state
// locks, so the graph itself does not need a mutex.
type transactionDAG struct {
	nodes          map[string]*dagNode
	producerByUtxo map[string]string
	spenderByUtxo  map[string]string
	nextSequence   uint64
}

type dagReadyQueue []*dagNode

func (q *dagReadyQueue) Len() int {
	return len(*q)
}

func (q *dagReadyQueue) Less(i, j int) bool {
	if (*q)[i].sequence != (*q)[j].sequence {
		return (*q)[i].sequence < (*q)[j].sequence
	}
	return (*q)[i].hash < (*q)[j].hash
}

func (q *dagReadyQueue) Swap(i, j int) {
	(*q)[i], (*q)[j] = (*q)[j], (*q)[i]
}

func (q *dagReadyQueue) Push(value any) {
	*q = append(*q, value.(*dagNode))
}

func (q *dagReadyQueue) Pop() any {
	old := *q
	last := len(old) - 1
	node := old[last]
	*q = old[:last]
	return node
}

func newTransactionDAG() *transactionDAG {
	return &transactionDAG{
		nodes:          make(map[string]*dagNode),
		producerByUtxo: make(map[string]string),
		spenderByUtxo:  make(map[string]string),
	}
}

func (d *transactionDAG) add(tx appliedTx) {
	if _, exists := d.nodes[tx.hash]; exists {
		return
	}
	node := &dagNode{
		hash:     tx.hash,
		sequence: d.nextSequence,
		parents:  make(map[string]struct{}),
		children: make(map[string]struct{}),
		consumed: slices.Clone(tx.consumed),
		produced: make([]string, 0, len(tx.created)),
	}
	d.nextSequence++
	for _, input := range tx.consumed {
		d.spenderByUtxo[input] = tx.hash
		if parentHash, ok := d.producerByUtxo[input]; ok {
			node.parents[parentHash] = struct{}{}
		}
	}
	for output := range tx.created {
		node.produced = append(node.produced, output)
		d.producerByUtxo[output] = tx.hash
	}
	slices.Sort(node.produced)
	d.nodes[tx.hash] = node
	for parentHash := range node.parents {
		if parent := d.nodes[parentHash]; parent != nil {
			parent.children[tx.hash] = struct{}{}
		}
	}
}

// descendants returns roots and every transitively dependent transaction.
func (d *transactionDAG) descendants(
	roots map[string]struct{},
) map[string]struct{} {
	ret := make(map[string]struct{}, len(roots))
	queue := make([]string, 0, len(roots))
	for hash := range roots {
		if _, ok := d.nodes[hash]; !ok {
			continue
		}
		ret[hash] = struct{}{}
		queue = append(queue, hash)
	}
	for len(queue) > 0 {
		hash := queue[0]
		queue = queue[1:]
		node := d.nodes[hash]
		if node == nil {
			continue
		}
		for childHash := range node.children {
			if _, seen := ret[childHash]; seen {
				continue
			}
			ret[childHash] = struct{}{}
			queue = append(queue, childHash)
		}
	}
	return ret
}

// remove deletes nodes and their indexes. Edges from surviving children are
// detached. This is correct for confirmed removal: the former parent's outputs
// are now resolved through ledger state instead of the pending graph.
func (d *transactionDAG) remove(hashes map[string]struct{}) {
	for hash := range hashes {
		node := d.nodes[hash]
		if node == nil {
			continue
		}
		for parentHash := range node.parents {
			if parent := d.nodes[parentHash]; parent != nil {
				delete(parent.children, hash)
			}
		}
		for childHash := range node.children {
			if child := d.nodes[childHash]; child != nil {
				delete(child.parents, hash)
			}
		}
		for _, input := range node.consumed {
			if d.spenderByUtxo[input] == hash {
				delete(d.spenderByUtxo, input)
			}
		}
		for _, output := range node.produced {
			if d.producerByUtxo[output] == hash {
				delete(d.producerByUtxo, output)
			}
		}
		delete(d.nodes, hash)
	}
}

func (d *transactionDAG) rebuild(applied []appliedTx) {
	replacement := newTransactionDAG()
	for _, tx := range applied {
		replacement.add(tx)
	}
	*d = *replacement
}

// topologicalOrder returns a stable Kahn traversal. Admission sequence is the
// primary ready-frontier key and transaction hash is a defensive tie-breaker.
func (d *transactionDAG) topologicalOrder() []string {
	indegree := make(map[string]int, len(d.nodes))
	ready := make(dagReadyQueue, 0, len(d.nodes))
	for hash, node := range d.nodes {
		indegree[hash] = len(node.parents)
		if len(node.parents) == 0 {
			heap.Push(&ready, node)
		}
	}
	ret := make([]string, 0, len(d.nodes))
	for len(ready) > 0 {
		node := heap.Pop(&ready).(*dagNode)
		ret = append(ret, node.hash)
		for childHash := range node.children {
			indegree[childHash]--
			if indegree[childHash] == 0 {
				if child := d.nodes[childHash]; child != nil {
					heap.Push(&ready, child)
				}
			}
		}
	}
	return ret
}
