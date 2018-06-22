# muFlow

**muFlow** (from *micro flow* or *multicore flow*) is a simple but powerful framework/language for executing your Python programs in parallel.

**muFlow** philosophy is to minimize your effort and increase reliability by constructing programs from atomic *tasks*.
Create small programs for your most common tasks and build large scripts for complex processing.
muFlow handles data exchange and parallelization for you!

### Basics

muFlow works by sequentially evaluating a script built of parameterizable *tasks* - your programs.
Tasks can take inputs and can produce outputs, exchanging data using named variables.
Effectively, a muFlow script is a directed acyclic graph (DAG) of tasks connected by data flow.

There are two types of tasks: *serial tasks* and *parallel tasks*.
Serial tasks run in a single main process - you can use them for example to list all files in a directory.
Parallel tasks are automatically run in parallel in subprocesses, iterating over lists produced by other tasks.

### Scopes and named variables

### Scripts and parameters

### Creating tasks

### Installation and dependencies
