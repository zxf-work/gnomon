# Data model

Gnomon stores a property graph whose every fact carries two independent
time dimensions. This page defines them, the three write operations, and how
a reader chooses which state of the graph it sees.

## Entities

* A **node** is identified by a string `uid`. It has a `label` and a JSON
  object of properties (`props`).
* An **edge** is identified by `(src, dst, rel_type, disc)`. `disc` is a
  discriminator that lets two distinct edges share endpoints and type (two
  messages between the same people, say); it defaults to the empty string
  for `assert_edge`, and to the event's position in the stream for bulk
  `ingest_events`. The engine derives a 96-bit edge id (`eid`) from those
  four fields.

Properties are stored as canonical JSON (sorted keys, compact separators),
byte for byte as written.

## Two clocks

| clock | interval | who sets it | meaning |
|---|---|---|---|
| valid time | `[vt_s, vt_e)` | the writer | when the fact holds in the modelled world |
| belief time (transaction time) | `[tt_s, tt_e)` | the store, at commit | when the store believed this version |

Both are `int64` microseconds since the Unix epoch, UTC. Intervals are
half-open. `OPEN_END = 2**62` stands for "no end yet": a valid interval that
is still open, or a version that is still believed.

Belief time comes from a hybrid logical clock owned by the store: each write
batch gets one transaction time, strictly greater than every earlier one,
close to the wall clock but never moving backwards. Because it is assigned
at commit, two independent loads of the same events receive different
transaction times; see [content digest vs store digest](architecture.md#digests).

## Versions

The store never updates a row. Each logical entity is a set of **versions**,
one row each:

```
identity | vt_s | vt_e | tt_s | tt_e | label / rel_type | props
```

A version is *believed at* transaction time `T` iff `tt_s <= T < tt_e`. At
any belief time the believed versions of one identity have disjoint valid
intervals. A version id (`vid`) is the hash of `(identity, tt_s, vt_s)`.

## Write operations

Every public write is one **batch**: it is appended to the write-ahead event
log, then applied to the engine at a single new transaction time `t`.

* **assert** (`assert_node`, `assert_edge`, `ingest_events`) — a new belief.
  Any believed version of the same identity whose valid interval overlaps
  the new one stops being believed at `t`; the parts of its valid interval
  that do not overlap are re-inserted with the old properties, believed from
  `t`. Then the new version is inserted.
* **retract** (`retract(ref, t_valid)`) — the fact ended at `t_valid`. The
  believed version whose valid interval contains `t_valid` stops being
  believed, and a copy with `vt_e = t_valid` is inserted.
* **correct** (`correct(ref, props, vt_s, vt_e)`) — the store was wrong about
  `[vt_s, vt_e)`. Believed versions overlapping that interval stop being
  believed; their non-overlapping remainders are preserved; a version with
  the corrected properties over `[vt_s, vt_e)` is inserted.

"Stops being believed" never rewrites the old row. It is recorded as data:
the batch appends a **close record** `(row, tt_e = t)` to an immutable close
run that the new manifest generation lists (see
[architecture](architecture.md#close-runs-and-sidecars)). The superseded
version therefore stays in the store forever and remains the answer to
"what did we believe before the correction?".

A version written and superseded inside the same batch would be believed
over the empty interval `[t, t)`; it is dropped instead of stored.

## Asking questions: current vs as-of

Every read operator except `diff_belief`, which compares two belief times
itself, takes `as_of_tt`, the belief state to evaluate under (`match` takes
it inside `scope`).
The default, `OPEN_END`, means the current beliefs. Valid-time arguments
(`window`, `t_valid`, `t1`/`t2`) independently select which facts hold in
the world. The two compose freely: "who did Alice work with in March, as we
believed it last Tuesday?" is a valid-time window plus an `as_of_tt`.

A worked example. Suppose these batches commit at transaction times
`T1 < T2 < T3`:

| batch | operation |
|---|---|
| `T1` | `assert_edge(alice, acme, WORKS_AT, {"role": "engineer"}, vt=[100, 500))` |
| `T2` | `correct(alice→acme WORKS_AT, {"role": "manager"}, vt=[300, 500))` |
| `T3` | `retract(alice→acme WORKS_AT, t=400)` |

The edge's version rows afterwards:

| vt | props | tt | believed now? |
|---|---|---|---|
| `[100, 500)` | engineer | `[T1, T2)` | no — closed by the correction |
| `[100, 300)` | engineer | `[T2, OPEN_END)` | yes — remainder kept by the correction |
| `[300, 500)` | manager | `[T2, T3)` | no — closed by the retraction |
| `[300, 400)` | manager | `[T3, OPEN_END)` | yes |

* Current beliefs (`as_of_tt = OPEN_END`): engineer over `[100, 300)`,
  manager over `[300, 400)`, nothing after 400.
* As of `T1`: engineer over `[100, 500)`.
* As of `T2`: engineer over `[100, 300)`, manager over `[300, 500)`.
* `version_history` with `belief: "superseded"` lists the two closed rows —
  the number of corrections made is a count of superseded rows.

The [quickstart](../README.md#quickstart) runs a smaller example — a
property correction on a node — end to end.

## Result envelopes

Operators return a self-describing envelope: the operator name, the
arguments with defaults filled in (`args_echo`), the store's current
valid-time extent, the payload, `truncated`/`cursor` for pagination, the
belief time the read was served at (`tt_q`, with `pinned` and `clamped`),
the later writes that could change the answer (`dependency`), the plan that
ran the call (`plan_ir`), and a
`result_digest` — the SHA-256 of the canonical JSON of the payload. Floats
are rounded to nine decimals before hashing, so the same store state and the
same arguments produce byte-identical results on every platform. Because the
digest covers the payload only, a result pinned to a past `as_of_tt` keeps
its digest under any later write. [docs/operators.md](operators.md#the-result-envelope)
shows a full envelope from a real call.
