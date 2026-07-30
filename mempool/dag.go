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
	"fmt"
	"slices"
)

// dagNode stores only graph/index metadata. Transaction bodies remain owned by
// Mempool so both implementations preserve the same immutable snapshot and
// relay behavior.
type dagNode struct {
	parents  map[string]struct{}
	children map[string]struct{}
	produced []string
}

// transactionDAG indexes producer/consumer relationships between pending
// transactions. All methods are called under Mempool's mutation and state
// locks, so the graph itself does not need a mutex.
type transactionDAG struct {
	nodes          map[string]*dagNode
	producerByUtxo map[string]string
	order          []string
}

func newTransactionDAG() *transactionDAG {
	return &transactionDAG{
		nodes:          make(map[string]*dagNode),
		producerByUtxo: make(map[string]string),
	}
}

func (d *transactionDAG) add(tx appliedTx) {
	if _, exists := d.nodes[tx.hash]; exists {
		return
	}
	node := &dagNode{
		parents:  make(map[string]struct{}),
		children: make(map[string]struct{}),
		produced: make([]string, 0, len(tx.created)),
	}
	for _, input := range tx.consumed {
		if parentHash, ok := d.producerByUtxo[input]; ok {
			node.parents[parentHash] = struct{}{}
		}
	}
	for output := range tx.created {
		node.produced = append(node.produced, output)
		d.producerByUtxo[output] = tx.hash
	}
	d.nodes[tx.hash] = node
	// A pending parent must already be admitted before a child can resolve its
	// output. Admission order is therefore itself a stable topological order.
	d.order = append(d.order, tx.hash)
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
		for _, output := range node.produced {
			if d.producerByUtxo[output] == hash {
				delete(d.producerByUtxo, output)
			}
		}
		delete(d.nodes, hash)
	}
	d.order = slices.DeleteFunc(d.order, func(hash string) bool {
		_, remove := hashes[hash]
		return remove
	})
}

func (d *transactionDAG) rebuild(applied []appliedTx) {
	replacement := newTransactionDAG()
	for _, tx := range applied {
		replacement.add(tx)
	}
	*d = *replacement
}

// topologicalOrder returns the cached stable ordering maintained at mutation
// time. A count mismatch is an internal index failure, not a representable
// partial answer: callers must diagnose it and use a complete fallback.
func (d *transactionDAG) topologicalOrder() ([]string, error) {
	ret := slices.Clone(d.order)
	if len(ret) != len(d.nodes) {
		return ret, fmt.Errorf(
			"DAG index inconsistent: %d of %d transactions ordered",
			len(ret),
			len(d.nodes),
		)
	}
	return ret, nil
}
