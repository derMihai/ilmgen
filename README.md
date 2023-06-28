# ILMGen - Generate ILM from raw HPCMP events

This program generates an *Iterative Lace Model* (ILM) from raw HPCMP events.

```
Usage: ilmgen <input> [<output>] [--dot] [--block-size <block-size>] [--pretty-json] [--no-trans-reduce] [--no-pruning]

Build ILM from raw HPCMP events

Positional Arguments:
  input             input file name, "-" for stdin
  output            output file name, defaults to "ilm.json" ("ilm.dot" if --dot
                    specified)

Options:
  --dot             output first DAG block as .dot file (Graphviz) instead of
                    json, then stop
  --block-size      DAG block maximal event count (default 4096)
  --pretty-json     formatted JSON (takes up more space)
  --no-trans-reduce skip transitive reduction of the event DAG
  --no-pruning      don't prune unobserved events
  --help            display usage information
```

Refer to the [Valgrind HPCMP tool](https://github.com/derMihai/valgrind/tree/main/hpcmp) for the HPCMP event format. For an example, see [test_input.json](test_input.json).

ILMGen is part of my master's thesis.

## Output

### JSON

In the standard JSON output, the ILM is represented as a series of blocks, each containing a DAG describing the precedence relation of synchronization events in the monitored application. Each event has quantitative and qualitative memory usage information attached. 

The base value is an array of `DagBlocks`:
```json
[
    DagBlock,
    DagBlock,
    ...
]
```

`DagBlock` contains a dictionary of `TID`s (threads IDs) as key with an array of `Event`s as value:
```json
{
    "tid1" : [ Event, Event, ... ],
    "tid2" : [ ... ],
    ...
}
```

`Event`s inside a thread are implicitly ordered by their precedence in the array. For two `DagBlock` $A$, $B$, if $A$ precedes $B$, then for any event $a\in A,\ b \in B$ $a$ precedes $b$ if and only if `TID_of(`$a$`)` $=$ `TID_of(`$b$`)`.  

`Event` looks as follows:

```json
{
  "id": u64,                    // Event ID, unique at least inside the current DagBlock.
  "icnt": u64,                  // Instruction count since last event within this thread.
  "hbal": i64,                  // Heap balance.
  "hmax": u64,                  // Heap maximum.
  "meta": str,                  // Event metadata for debugging, may be anything.
  "nxt": [ NextBlock, ... ],    // Events in OTHER threads ordered after this one.
  "bks": {
    "new": [ MemBlock, ...],    // New memory allocations since last event within this thread.
    "del": [ BlockUsage, ...]   // Freed allocations since last event within this thread.
  },
  "bksu": [ BlockUsage, ... ]   // Memory usage counters since last event within this thread, for 
                                // allocations that are still alive.
},
```

Heap balance (`hbal`) is the sum of the allocations and de-allocation since the last event within same thread, and thus can be negative.  
Heap maximum (`hmax`) is the allocation peak reached by this thread since the last event within same thread.  
`NextBlock`:
```json
{
    "thid": u64,  // TID of the thread of the event.
    "id": u64     // Event ID.
}
```
`MemBlock`:
```json
{
  "addr": u64, // Start address of the allocation.
  "size": u64  // Allocation size [Bytes].
}
```
`BlockUsage`:
```json
{
  "addr": u64, // Start address of the allocation.
  "size": u64, // Allocation size [Bytes].
  "r": u64,    // Reads performed by current thread on this allocation since last event [Bytes].
  "w": u64     // Stores performed by current thread on this allocation since last event [Bytes].
}
```

### Dot
When the `--dot` command line switch is specified, no JSON representation is exported, but a `.dot` for visualizing the
precedence relation of the events in the **first** `DagBlock`. It is only for debugging the ordering of events. Refer to the [Graphviz](https://graphviz.org/) documentation for means of visualizing the `.dot` file.