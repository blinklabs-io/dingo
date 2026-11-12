# Bursa KES agent golden frames

Copied from `kesagent/wire/testdata/frames` in the `github.com/blinklabs-io/bursa`
module version that `go.mod` requires. Only the frames this client reads or
writes are copied; the control-socket frames are not.

`TestWireGoldenFrames` decodes each `.hex` frame with this package's framing,
compares it with the `.json`, and re-encodes it byte for byte. When the bursa
requirement changes, copy the frames again from that version:

```sh
M=$(go list -m -f '{{.Dir}}' github.com/blinklabs-io/bursa)
for f in kesagent/testdata/wireframes/*.hex; do
	n=$(basename "$f" .hex)
	install -m 0644 "$M/kesagent/wire/testdata/frames/$n".{hex,json} \
		kesagent/testdata/wireframes/
done
```
