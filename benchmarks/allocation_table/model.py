#!/usr/bin/env python3
"""Bounded executable models for allocation-table design; no timing claims.

The packed model is sequential: it does NOT prove a concurrent two-structure
publication/reclamation algorithm. The checkpoint model takes an exact durable
root/log ownership graph as an oracle. It verifies publication ordering and
retention for that model; deriving this graph incrementally from SplinterDB's
live references is deliberately outside its scope. Metadata uses a reserved
arena, and device persistence is modeled as atomic durable objects plus an
invalid (torn) manifest. Real writeback, fsync, checksums, and sector tearing
need implementation-specific crash tests. Old manifests are eligible only until
a newer publication is confirmed durable; arbitrary later media corruption is
not allowed to resurrect an old root after its extents have been reused.

Run: python3 benchmarks/allocation_table/model.py
"""

import json
import random
from collections import Counter
from dataclasses import dataclass


if not __debug__:
    raise SystemExit("model.py requires assertions; run Python without -O/-OO")


MAX_REF = (1 << 32) - 1
FREE, NO_REFS, ONE_REF, ESCAPE = range(4)


class PackedCounts:
    """Two-bit tags; tag 3 redirects to an exact uint32 count >= 3."""

    def __init__(self, capacity, reserved=0):
        self.capacity = capacity
        self.reserved = reserved
        self.words = [0] * ((capacity + 31) // 32)
        self.large = {}
        for extent in range(reserved):
            self._set(extent, ONE_REF)

    def tag(self, extent):
        assert 0 <= extent < self.capacity
        return (self.words[extent // 32] >> (2 * (extent % 32))) & 3

    def get(self, extent):
        tag = self.tag(extent)
        return self.large[extent] if tag == ESCAPE else tag

    def _set(self, extent, count):
        assert 0 <= extent < self.capacity
        assert 0 <= count <= MAX_REF
        word, shift = extent // 32, 2 * (extent % 32)
        if count >= ESCAPE:
            self.large[extent] = count
            tag = ESCAPE
        else:
            tag = count
            self.large.pop(extent, None)
        mask = 3 << shift
        self.words[word] = (self.words[word] & ~mask) | (tag << shift)

    def allocate(self, extent):
        assert extent >= self.reserved and self.get(extent) == FREE
        self._set(extent, ONE_REF)
        return ONE_REF

    def inc(self, extent):
        count = self.get(extent)
        assert extent >= self.reserved and 0 < count < MAX_REF
        self._set(extent, count + 1)
        return count + 1

    def dec(self, extent):
        count = self.get(extent)
        assert extent >= self.reserved and count > 0
        self._set(extent, count - 1)
        return count - 1

    def validate(self):
        escapes = {x for x in range(self.capacity) if self.tag(x) == ESCAPE}
        assert escapes == self.large.keys()
        assert all(ESCAPE <= count <= MAX_REF for count in self.large.values())
        assert all(self.get(x) == ONE_REF for x in range(self.reserved))
        assert all(0 <= word < 1 << 64 for word in self.words)


def expect_assertion(fn):
    try:
        fn()
    except AssertionError:
        return
    raise AssertionError("invalid operation was accepted")


def packed_boundaries():
    packed = PackedCounts(65, reserved=1)
    for extent in (1, 31, 32, 63, 64):
        assert packed.allocate(extent) == ONE_REF
        assert packed.inc(extent) == ESCAPE
        assert packed.get(extent) == 3
        assert packed.dec(extent) == ONE_REF
        assert extent not in packed.large
        assert packed.dec(extent) == NO_REFS
        assert packed.inc(extent) == ONE_REF
        assert packed.dec(extent) == NO_REFS
        assert packed.dec(extent) == FREE
        assert packed.allocate(extent) == ONE_REF
    packed._set(32, 1 << 24)
    assert packed.inc(32) == (1 << 24) + 1
    packed._set(32, MAX_REF)
    expect_assertion(lambda: packed.inc(32))
    assert packed.get(32) == MAX_REF
    assert packed.dec(32) == MAX_REF - 1
    expect_assertion(lambda: packed.dec(0))
    expect_assertion(lambda: packed.allocate(0))
    expect_assertion(lambda: packed.inc(2))
    expect_assertion(lambda: packed.dec(2))
    expect_assertion(lambda: packed.get(65))
    packed.validate()
    return {"word_boundaries": [31, 32, 63, 64], "max_count": MAX_REF}


def packed_differential():
    operations = Counter()
    reused = 0
    for seed in (0, 1, 17, 42):
        rng = random.Random(seed)
        packed = PackedCounts(257, reserved=7)
        reference = [ONE_REF] * 7 + [FREE] * 250
        allocated_before = set()
        for step in range(20000):
            extent = rng.randrange(7, len(reference))
            count = reference[extent]
            if count == FREE:
                reference[extent] = ONE_REF
                result = packed.allocate(extent)
                operations["allocate"] += 1
                reused += extent in allocated_before
                allocated_before.add(extent)
            elif rng.randrange(5) < 2:
                reference[extent] += 1
                result = packed.inc(extent)
                operations["increment"] += 1
            elif rng.randrange(4) < 3:
                reference[extent] -= 1
                result = packed.dec(extent)
                operations["decrement"] += 1
            else:
                result = packed.get(extent)
                operations["read"] += 1
            assert result == reference[extent]
            if step % 251 == 0:
                packed.validate()
                assert [packed.get(x) for x in range(257)] == reference
        packed.validate()
        assert [packed.get(x) for x in range(257)] == reference
    assert reused > 0 and all(operations.values())
    return {"seeds": [0, 1, 17, 42], "operations": dict(operations),
            "extent_reuses": reused}


def fuzzy_counterexamples():
    # Copy B before allocation, then A after a root swap and reclamation.
    live = {"A": ONE_REF, "B": FREE}
    captured = {"B": live["B"]}
    live["B"] = ONE_REF
    durable_root = {"B": ONE_REF}
    live["A"] = FREE
    captured["A"] = live["A"]
    assert captured == {"A": FREE, "B": FREE}
    assert captured["B"] < durable_root["B"]

    # Even a point-in-time live map includes a reader's extra reference and
    # an unpublished allocation; neither belongs in exact recovery counts.
    exact = {"A": ONE_REF, "B": FREE}
    live = {"A": ONE_REF + 1, "B": ONE_REF}
    assert live != exact
    assert all(live[x] >= exact[x] for x in exact)
    # Releasing the sole durable ownership in the overcounted recovered map
    # leaves a logical reference, whereas the exact map reaches NO_REFS.
    assert live["A"] - 1 == ONE_REF
    assert exact["A"] - 1 == NO_REFS
    return ["fuzzy copy can omit the new durable root's extent",
            "consistent live copy can leak temporary references and orphans"]


@dataclass(frozen=True)
class Root:
    # Includes the root's entire durable graph AND retained log/blob ownership.
    name: str
    references: tuple  # (extent address, logical references, content token)

    def counts(self, capacity):
        counts = [FREE] * capacity
        for extent, logical_refs, token in self.references:
            assert logical_refs > 0 and token
            assert counts[extent] == FREE
            counts[extent] = logical_refs + 1
        return tuple(counts)

    def extents(self):
        return {extent for extent, _, _ in self.references}

    def contents(self):
        return {extent: token for extent, _, token in self.references}


@dataclass(frozen=True)
class Manifest:
    sequence: int
    root: str
    directory: str
    valid: bool = True


class CheckpointStore:
    """Finite atomic-object durability model, with immutable chunk sharing."""

    def __init__(self):
        self.capacity = 12
        self.chunk_size = 4
        self.data = {}  # extent address -> durable content/incarnation token
        self.roots = {}
        self.chunks = {}
        self.directories = {}
        self.manifests = []
        self.protected = {}  # runtime references, intentionally not persisted

    def retain(self, root):
        self.protected[root.name] = root

    def release(self, root):
        del self.protected[root.name]

    def eligible_manifests(self):
        # Only the newest complete publication is selected. This finite
        # failure model tears candidates before publication; it does not
        # corrupt already-confirmed durable manifests after storage reuse.
        valid = [m for m in self.manifests if m.valid]
        if not valid:
            return []
        newest = max(m.sequence for m in valid)
        return [m for m in valid if m.sequence == newest]

    def reusable(self, extent):
        durable_roots = [self.roots[m.root] for m in self.eligible_manifests()]
        return all(extent not in root.extents()
                   for root in list(self.protected.values()) + durable_roots)

    def write_content(self, extent, token):
        if extent in self.data and self.data[extent] != token:
            assert self.reusable(extent), "attempt to overwrite a retained extent"
        self.data[extent] = token

    def validate_root_contents(self, root):
        for extent, token in root.contents().items():
            assert self.data.get(extent) == token, "missing or reused root extent"

    def materialize(self, directory):
        return sum((self.chunks[key] for key in self.directories[directory]), ())

    def recover(self):
        eligible = self.eligible_manifests()
        assert eligible, "no valid publication"
        manifest = max(eligible, key=lambda m: m.sequence)
        root = self.roots[manifest.root]
        self.validate_root_contents(root)
        counts = self.materialize(manifest.directory)
        assert counts == root.counts(self.capacity)
        return manifest, counts

    def publish(self, manifest):
        # The only authoritative publication step. Required dependencies must
        # already be durable; this check models the ordering contract.
        previous = max((m.sequence for m in self.manifests if m.valid), default=0)
        assert manifest.valid and manifest.sequence > previous
        root = self.roots[manifest.root]
        self.validate_root_contents(root)
        assert self.materialize(manifest.directory) == root.counts(self.capacity)
        self.manifests.append(manifest)


def checkpoint_crash_prefixes():
    old = Root("old", ((1, 1, "old-1"), (5, 2, "shared-5"), (9, 1, "old-9")))
    new = Root("new", ((2, 1, "new-2"), (5, 2, "shared-5"), (10, 1, "new-10")))
    late = Root("late-live-only", ((3, 1, "late-3"),))
    store = CheckpointStore()
    old_counts, new_counts = old.counts(12), new.counts(12)
    store.data.update(old.contents())
    store.roots[old.name] = old
    old_keys = []
    for chunk in range(3):
        key = f"old-{chunk}"
        store.chunks[key] = old_counts[chunk * 4:(chunk + 1) * 4]
        old_keys.append(key)
    store.directories["old-map"] = tuple(old_keys)
    store.publish(Manifest(1, old.name, "old-map"))
    store.retain(old)
    store.retain(new)
    store.retain(late)
    versions = []

    def crash_at(label):
        manifest, recovered = store.recover()
        versions.append([label, manifest.sequence])
        assert recovered[3] == FREE  # late volatile allocation is absent
        assert not store.reusable(1) and not store.reusable(2)
        assert not store.reusable(3)
        assert store.materialize("old-map") == old_counts

    crash_at("before writes")
    store.write_content(2, "new-2")
    crash_at("first new data object durable")
    store.write_content(10, "new-10")
    crash_at("remaining new data durable")
    store.roots[new.name] = new
    crash_at("new root object durable")

    # The middle immutable chunk is shared; changed chunks are written to new
    # addresses and cannot damage the map still named by the old manifest.
    new_keys = []
    for chunk in range(3):
        values = new_counts[chunk * 4:(chunk + 1) * 4]
        if values == store.chunks[old_keys[chunk]]:
            new_keys.append(old_keys[chunk])
        else:
            key = f"new-{chunk}"
            store.chunks[key] = values
            new_keys.append(key)
            crash_at(f"new map chunk {chunk} durable")
    assert new_keys[1] == old_keys[1]
    store.directories["new-map"] = tuple(new_keys)
    crash_at("new map directory durable")
    store.manifests.append(Manifest(2, new.name, "new-map", valid=False))
    crash_at("torn manifest write")
    store.publish(Manifest(2, new.name, "new-map"))
    crash_at("valid manifest durable")
    assert all(sequence == 1 for _, sequence in versions[:-1])
    assert versions[-1][1] == 2

    # The old slot remains physically present but is now ineligible. Reader
    # pins separately prevent reuse until those readers have drained.
    assert any(m.sequence == 1 and m.valid for m in store.manifests)
    assert all(m.sequence == 2 for m in store.eligible_manifests())
    expect_assertion(lambda: store.write_content(1, "reused-1"))
    store.release(old)
    assert store.reusable(1) and store.reusable(9)
    store.write_content(1, "reused-1")
    store.write_content(9, "reused-9")
    expect_assertion(lambda: store.validate_root_contents(old))
    manifest, recovered = store.recover()
    assert manifest.sequence == 2 and recovered == new_counts
    assert not store.reusable(5)  # still belongs to the new durable root
    store.release(late)
    assert store.reusable(3)

    return {"crash_prefixes": versions, "shared_chunks": 1,
            "changed_chunks": 2,
            "reuse": "two old-only extents overwritten after newer durable publication and reader release",
            "old_slot": "older slot is not selected after newer confirmed publication",
            "failure_scope": "candidate tearing before publication; no later media-corruption rollback guarantee"}


def checkpoint_rejects_bad_order():
    store = CheckpointStore()
    root = Root("root", ((1, 1, "original-1"),))
    store.roots[root.name] = root
    for chunk in range(3):
        store.chunks[str(chunk)] = root.counts(12)[chunk * 4:(chunk + 1) * 4]
    store.directories["map"] = ("0", "1", "2")
    expect_assertion(lambda: store.publish(Manifest(1, root.name, "map")))
    store.write_content(1, "original-1")
    store.chunks["wrong"] = (FREE,) * 4
    store.directories["wrong-map"] = ("wrong", "1", "2")
    expect_assertion(lambda: store.publish(Manifest(1, root.name, "wrong-map")))
    store.publish(Manifest(1, root.name, "map"))
    assert store.recover()[0].sequence == 1
    # Bypass the retention guard to model a faulty allocator reusing an
    # address early: address membership still holds, but content changed.
    expect_assertion(lambda: store.write_content(1, "early-overwrite-1"))
    store.data[1] = "early-overwrite-1"
    assert root.extents() <= store.data.keys()
    expect_assertion(store.recover)
    return ["publication before referenced data durability",
            "publication with a map that does not match the durable graph",
            "recovery after early overwrite of a still-eligible root extent"]


def main():
    results = {
        "packed_boundaries": packed_boundaries(),
        "packed_differential": packed_differential(),
        "counterexamples": fuzzy_counterexamples(),
        "checkpoint_crash_prefixes": checkpoint_crash_prefixes(),
        "checkpoint_rejects_bad_order": checkpoint_rejects_bad_order(),
    }
    print(json.dumps({"status": "PASS", "models": results,
                      "scope": "sequential representation and abstract crash ordering; no performance measurement"},
                     indent=2))


if __name__ == "__main__":
    main()
