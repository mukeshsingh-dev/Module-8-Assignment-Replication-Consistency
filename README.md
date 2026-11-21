# Replication Consistency Lab

This lab simulates a small replicated inventory system with one Primary replica and one Secondary replica. Writes go to the Primary first, then a background replication worker applies the update to the Secondary. A deliberate delay makes the stale-read window observable.

## Architecture

```text
                    write stock
                 +--------------+
                 |    Client    |
                 +------+-------+
                        |
                        v
                 +--------------+
                 |    Primary   |
                 |   stock=10   |
                 +------+-------+
                        |
             asynchronous replication
              (delayed in stale-read test)
                        v
                 +--------------+
                 |   Secondary  |
                 |   stock=10   |
                 +--------------+
                        ^
                        |
                 reads can target
                 either replica
```

## Requirements

- Python 3.9 or newer
- No third-party packages

## Run the simulation

```bash
python3 replication_consistency.py
```

## Evidence

The output below was produced by running the program successfully:

```text
=== Replication Consistency Demo ===
[START] Primary stock=10
[START] Secondary stock=10

[TEST 1] Normal replication
[REPLICATION] Secondary received stock=10
[READ] Primary=10 Secondary=10
[PASS] Replicas return the same value after replication completes.

[TEST 2] Stale read during delayed replication
[READ IMMEDIATELY] Primary=9 Secondary=10
[PASS] Secondary temporarily returned stale stock=10 while Primary returned stock=9.

[TEST 3] Recovery and convergence
[REPLICATION] Secondary received stock=9
[READ AFTER RECOVERY] Primary=9 Secondary=9
[PASS] Replicas converged to stock=9 after replication resumed.

=== ALL TESTS PASSED ===
```

The stale-read evidence is the line `[READ IMMEDIATELY] Primary=9 Secondary=10`: the Primary has the new value while the delayed Secondary still serves the old value. After the replication worker finishes, both replicas return `9`.

The captured terminal output is also available in [`run-output.txt`](run-output.txt).

## Reflection

Reading from a nearby replica can be faster and can reduce load on the Primary. The tradeoff is that asynchronous replication may temporarily return an older value, so the reader is not guaranteed to see the latest write. Reading from the Primary or waiting for a replication/convergence signal improves freshness but adds latency and can reduce availability. A system chooses between these costs based on whether speed, consistency, or availability matters most for the operation.
