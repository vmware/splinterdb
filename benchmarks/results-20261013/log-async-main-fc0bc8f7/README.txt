Automatic asynchronous log barriers on main fc0bc8f7

Source parent:9bc5ebf4, the preserved synchronous-policy merge of ad751b31 and fc0bc8f7.
Reuses existing async core barrier; short submit guard is released before durability.
No new worker or retry engine. Automatic and fire-and-forget errors are logged.

Validation: debug core43 initially, plus newly added rejection/retry case;
release core44. Both modes quick56, process-exit1 covering3 data shapes,
and functional log500000 records pass. Final refinements rerun both modes:
blocked-device gate reached BEFORE successor insert/manual barrier; process-exit
uses actual background worker and drains accepted tasks without a new barrier.
The zero-test debug-final-automatic.log invocation is preserved but not counted.
Full format check and whitespace pass. Independent code review found no blocker.
Runtime benchmark evidence is added separately once complete.
