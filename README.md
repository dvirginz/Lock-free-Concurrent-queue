## Kiwi PQ ##
We present a novel usage for the KiWi - Ordered Key-Value Map data structure (Basin et al., 2017)
as priority queue in C++ and evaluate it against OBIM in the Galois framework as the task
scheduler for the SSSP (single source shortest path) task .
KiWi uses ”CHUNKS” as its base unit while handling scans and rebalance steps;
In KiWiPQ we reuse the same mechanism to reduce the dequeueing contention by having each thread dequeue an
entire chunk, and dequeue new work items from the (thread local) detached chunk.
