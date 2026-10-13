# Prefetch admission repair on the handoff stack

The merge `65b9123` inherits project 1's narrow unpublished-prefetch guard.
These runs passed the deterministic regression, the async cache driver,
16 writeback tests (including cache handle transfer), and 48 public quick
tests (including iterator transfer). The manifest records source, exact
commands, and the debug executable/shared-library hashes.
