## Problem

When multiple workers process messages for the same key concurrently, they operate without knowledge of each other’s progress.
This can lead to race conditions where one worker’s result overwrites another’s, causing data loss or skipped results.

## Current Solution

To avoid conflicts, each key is exclusively assigned to a single worker.

This ensures no two workers process messages for the same key at the same time.

Conflicts are eliminated since updates are performed sequentially per key.

I also changed the storage from a single array to a Map.
This makes finding messages for a specific key much faster, since we don’t have to search through the whole list. It does make the size function a bit more complex, but since size is called far less often, the impact is small.

## Limitation

If the number of workers is greater than the number of unique keys:

Only number_of_keys workers can be active at any time.

Remaining workers will stay inactive, reducing overall throughput.

## Possible Improvement

A more scalable approach would be to implement atomic operations at the database level for each key.

This would allow multiple workers to process messages for the same key concurrently.

Eliminates the need to block workers when the worker count exceeds the number of keys.
