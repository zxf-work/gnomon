# Changelog

## 0.1.1

A fix to the identity filter of the native storage engine. No other engine
change.

**Identity-filter files are now checked byte for byte when they are
loaded.** The native engine keeps one identity-filter file per segment under
`native/idx/vidfilter/`. Reads of one entity, and the lookup that `correct`,
`retract`, `assert_edge` and `assert_node` make before replacing a version,
skip every segment whose filter says it holds no row of that entity. In 0.1.0
the checksum of a filter file covered the filter array but not the file's
header, so some damage to the header was accepted, for example one changed
digit of the filter's `seed`. Such a filter then answered "not here" for rows
its segment holds:

* `entity_history`, `version_history` with `ref`, and `match` and
  `snapshot_subgraph` from given nodes returned wrong answers;
* `correct` and `retract` failed for a version that exists;
* `assert_edge` and `assert_node` over an existing version left the old
  version believed next to the new one.

`gnomon store verify` reported such a store healthy; only after the
`assert_edge` or `assert_node` case did `--full` report the overlapping
versions.

In 0.1.1 the checksum covers every byte of the file before its footer, the
footer is checked by value, and the checksum is checked before any header
field is read. A file that fails any check is ignored: answers come from the
segments, and the file is rebuilt.

**Upgrading.** The filter file format number moves from 1 to 2, so 0.1.1
ignores the filter files written by 0.1.0 in the same way and rebuilds them.
A writable handle rebuilds a segment's filter file the first time a read
needs it; opening and closing a store rewrites none.
`gnomon store build-indexes --vidfilter` rebuilds all of them at once. A
read-only handle builds what it needs in memory and writes nothing. Nothing
else in a store changes: the event log, segments, manifests and their
format, the dictionary, store identity, the digests and every answer on an
undamaged store are the same as with 0.1.0. A store used by 0.1.1 still
opens in 0.1.0 with the same answers: 0.1.0 ignores the new filter files, and
its writable handles and index builds write old-format files in their place,
which 0.1.1 then rebuilds again.

**Known limit.** The entity dictionary file (`native/dict.log`) is not
covered by a checksum in 0.1.x; see [Verification](docs/architecture.md#verification).

## 0.1.0

First public release.
