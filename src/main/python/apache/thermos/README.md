** This document is deprecated and no longer updated. **

# Thermos manual #

[TOC]


## tl;dr ##

** You noticed the deprecation notice, right? **

### What is Thermos? ###

Thermos a simple process management framework used for orchestrating
dependent processes within a single chroot.  At Twitter, it is used as a
process manager for Mesos tasks.  In practice, there is a one-to-one
correspondence between a Mesos task and Thermos task.  This document
describes how to use Thermos in a local development environment and does not
describe how to run Thermos tasks on Mesos, though once you have a valid
Thermos configuration doing so is a small step.

### Simplest invocation ###

Thermos lives in `science` at Twitter and publically on GitHub in `twitter-commons` (TBD).

#### build ####

Build the Thermos CLI and the Thermos observer.

```shell
$ ./pants src/python/apache/thermos
$ ./pants src/python/apache/thermos/bin:thermos_observe
```

You can copy `dist/thermos.pex` to `thermos` somewhere in your `$PATH` or use a la carte.

#### simplerun ####

You can run Thermos tasks without first writing a configuration file using `simplerun`:

```shell
$ thermos simplerun 'echo hello world'
Running command: 'echo hello world'
 INFO] Forking Process(simple)
 INFO] Process(simple) finished successfully [rc=0]
 INFO] Killing task id: simple-20120529-162532.018646
 INFO]     => Current user: wickman
 INFO] Kill complete.
 INFO] Task succeeded.
```

#### introspection ####

```shell
$ thermos status --verbosity=3 simple-20120529-162532.018646
Found task simple-20120529-162532.018646
  simple-20120529-162532.018646 [owner:  wickman]  state:  SUCCESS start:  Tue May 29 16:25:32 2012
    user: wickman ports: None
    sandbox: None
    process table:
      - simple runs: 1 last: pid=57471, rc=0, finish:Tue May 29 16:25:32 2012, state:SUCCESS

$ thermos tail simple-20120529-162532.018646
Tail of terminal log /Users/wickman/.thermos/logs/simple-20120529-162532.018646/simple/0/stdout
hello world
```

#### thermos observer ####

```shell
$ dist/thermos_observe.pex --root=$HOME/.thermos
```

This will fire up a webserver on `localhost:1338` that you can use as a web interface to interact with
locally running Thermos tasks.


## Building blocks ##

Thermos is made of Processes and Tasks.

### Thermos processes ###

A Thermos process is simply a command-line that is invoked in a subshell.  A single
run of a process may succeed (have a zero exit status) or fail (have a
non-zero exit status.) A process is considered permanently failed after a
maximum number of individual run failures, by default one.

### Thermos tasks ###

A Thermos task is a collection of processes and constraints that dictate how
and when to run them.  By default there are no constraints between processes
bound to a task and they run in parallel.  The simplest (and currently only)
Task level constraint is the `order` dependency: process B should not be run
until process A has completed successfully.  For example, process A could be
`git clone` and process B could be `rake test`.  It doesn't make sense to
run process B until process A is fully completed and successful.


### Thermos configuration ###

The easiest way to invoke Thermos is using `thermos simplerun`.  Under the
covers, this synthesizes a Thermos task with a single Thermos process named
`simple`.  To do the same thing via Thermos configuration, add
the following to `simple.thermos`:

```python
process = Process(name = 'simple', cmdline = 'echo hello world')
task = Task(name = 'simple', processes = [process])
export(task)
```

Then invoke by `thermos run simple.thermos`.

Configuring Thermos is done through
[pystachio](http://github.com/wickman/pystachio) templates.  These templates
behave like structurally typed collections of key-value pairs.  You can
either construct configuration through plain old Python objects (as above)
or as Python dictionaries or JSON that is coerced into Python dictionaries.

For example, the above configuration is equivalent to:

```python
export({
  'name': 'simple',
  'processes': [{
    'name': 'simple',
    'cmdline': 'echo hello world'
  }]
})
```

The full Thermos pystachio schema can be found at
`src/python/apache/thermos/config/schema.py` and is mostly described below.


## Configuration reference ##

### Process objects ###

Processes fundamentally consist of a `name`, `cmdline`.  You will rarely
need to specify anything more.

<table>
  <tr> <td colspan=2> <b>Process schema</b> </td> </tr>
  <tr> <td> <em>name</em> (required)    </td> <td> Process name (String) </td> </tr>
  <tr> <td> <em>cmdline</em> (required) </td> <td> Command line (String) </td> </tr>
  <tr> <td> <em>max_failures</em>       </td> <td> Max failures (Integer, default 1) </td> </tr>
  <tr> <td> <em>daemon</em>             </td> <td> Daemon process? (Boolean, default False) </td> </tr>
  <tr> <td> <em>ephemeral</em>          </td> <td> Ephemeral process? (Boolean, default False) </td> </tr>
  <tr> <td> <em>min_duration</em>       </td> <td> Min duration between runs in seconds (Integer, default 15) </td> </tr>
  <tr> <td> <em>final</em>              </td> <td> This is a finalizing process that should run last (Boolean, default False) </td> </tr>
</table>


#### name ####

The name is any string that is a valid UNIX filename (specifically no
slashes, NULLs or leading periods.) Each process name within a task must be
unique.

#### cmdline ####

The command line is invoked in a bash subshell, so can be full-blown bash
scripts, though no command-line arguments are supplied.

#### max_failures ####

The maximum number of failures (non-zero exit statuses) this process can
sustain before being marked permanently failed and not retried.  If a
process becomes permanently failed, Thermos looks to the task failure limit
(usually 1) to determine whether or not the Thermos task should be failed.

Setting max_failures to 0 means that this process will be retried
indefinitely until a successful (zero) exit status is achieved.  It will be retried
at most once every `min_duration` seconds in order to prevent DoSing the
coordinating thermos scheduler.

#### daemon ####

By default Thermos processes are non-daemon.  If `daemon` is set to True, a
successful (zero) exit status will not prevent future process runs. 
Instead, the process will be reinvoked after min_duration seconds.  However,
the maximum failure limit will still apply.  A combination of `daemon=True`
and `max_failures=0` will cause a process to be retried indefinitely
regardless of exit status.  This should generally be avoided for very
short-lived processes because of the accumulation of checkpointed state for
each process run.

#### ephemeral ####

By default Thermos processes are non-ephemeral.  If `ephemeral` is set to
True, the status of the process is not used to determine whether the task in
which it is bound has completed.  Take for example a Thermos task with a
non-ephemeral webserver process and an ephemeral logsaver process that
periodically checkpoints its log files to a centralized data store.  The
task is considered finished once the webserver process has completed,
regardless of the current status of the logsaver.

#### min_duration ####

Processes may succeed or fail multiple times throughout the duration of a
single task.  Each of these is called a "process run."  The `min_duration` is the minimum
number of seconds the scheduler waits between running the same process.

#### final ####

Processes can be grouped into two classes: ordinary processes and finalizing
processes.  By default, Thermos processes are ordinary.  They run as long as
the Thermos Task is considered healthy (i.e., no failure limits have been
reached.) But once all regular Thermos processes have either finished or the
Task has reached a certain failure threshold, it moves into a "finalization"
stage and then runs all finalizing processes.  These are typically processes
necessary for cleaning up the task, such as log checkpointers, or perhaps
e-mail notifications that the task has completed.

Finalizing processes may not depend upon ordinary processes or vice-versa, however
finalizing processes may depend upon other finalizing processes and will otherwise run as
a typical process schedule.


### Task objects ###

Tasks fundamentally consist of a `name` and a list of processes `processes`.
Processes can be further constrained with `constraints`

<table>
  <tr> <td colspan=2> <b>Task schema</b> </td> </tr>
  <tr> <td> <em>name</em> (required)       </td> <td> Task name (String) </td> </tr>
  <tr> <td> <em>processes</em> (required)  </td> <td> List of processes (List of Process objects) </td> </tr>
  <tr> <td> <em>constraints</em>           </td> <td> Constraints (List of Constraint objects, default []) </td> </tr>
  <tr> <td> <em>resources</em>             </td> <td> Resource footprint (Resource, optional) </td> </tr>
  <tr> <td> <em>max_failures</em>          </td> <td> Max failures (Integer, default 1) </td> </tr>
  <tr> <td> <em>max_concurrency</em>       </td> <td> Max concurrency (Integer, default 0 = unlimited concurrency) </td> </tr>
  <tr> <td> <em>finalization_wait</em>     </td> <td> Amount of time allocated to run finalizing processes (Integer in seconds, default 30) </td> </tr>
</table>

<table>
  <tr> <td colspan=2> <b>Constraint schema</b> </td> </tr>
  <tr> <td> <em>order</em> </td> <td> List of process names that should run in order (List of Strings)</td> </tr>
</table>

<table>
  <tr> <td colspan=2> <b>Resource schema</b> </td> </tr>
  <tr> <td> <em>cpu</em> (required) </td> <td> Number of cores (Float) </td> </tr>
  <tr> <td> <em>ram</em> (required) </td> <td> Bytes of RAM (Integer) </td> </tr>
  <tr> <td> <em>disk</em> (required) </td> <td> Bytes of disk (Integer) </td> </tr>
</table>

#### name ####

The name is used to label the task and is used for reporting in the observer UI and for
management in the thermos CLI.

#### processes ####

Processes is an unordered list of `Process` objects.  In order to place temporal constraints upon
them, you must use `constraints`.

#### constraints ####

A list of `Constraint` objects.  Currently only one type of constraint is supported, the `order` constraint.
`order` is a list of process names that should run in order.  For example,

```python
process = Process(cmdline = "echo hello world")
task = Task(name = "echoes", processes = [process(name = "first"), process(name = "second")],
            constraints = [Constraint(order = ["first", "second"]))
```

Constraints can be supplied ad-hoc and in duplicate and not all processes need be constrained, however
tasks with cycles will be rejected by the Thermos scheduler.

#### resources ####

`resources` is a `Resource` object described by `cpu`, `ram`, and `disk`.  It is currently unused by
Thermos but reserved for future use in constraining the resource consumption of a task.

#### max_failures ####

`max_failures` is the number of failed processes in order for this task to be marked as failed.  A single
process run does not consistute a failure.  For example:

```python
template = Process(max_failures=10)
task = Task(name = "fail", processes = [template(name = "failing", cmdline = "exit 1"),
                                        template(name = "succeeding", cmdline = "exit 0")],
            max_failures=2)
```

The `failing` process would fail 10 times before being marked as permanently
failed, and the `succeeding` process would succeed on the first run.  The
task would succeed despite only allowing for two failed processes.  To be
more specific, there would be 10 failed process runs yet 1 failed process.

#### max_concurrency ####

For tasks with a number of expensive but otherwise independent processes, it
may be desirable to limit the amount of concurrency provided by the Thermos
scheduler rather than artificially constraining them through `order`
constraints.  For example, a test framework may generate a task with 100
test run processes, but would like to run it on a machine with only 4 cores.
You can limit the amount of parallelism to 4 by setting `max_concurrency=4`
in your task configuration.

For example, the following Thermos task spawns 180 processes ("mappers") to compute
individual elements of a 180 degree sine table, all dependent upon one final process ("reducer")
to tabulate the results:

```python
def make_mapper(id):
  return Process(
    name = "mapper%03d" % id,
    cmdline = "echo 'scale=50;s(%d*4*a(1)/180)' | bc -l > temp.sine_table.%03d" % (id, id))

def make_reducer():
  return Process(name = "reducer",
                 cmdline = "cat temp.* | nl > sine_table.txt && rm -f temp.*")

processes = map(make_mapper, range(180))

task = Task(
  name = "mapreduce",
  processes = processes + [make_reducer()],
  constraints = [Constraint(order = [mapper.name(), 'reducer']) for mapper in processes],
  max_concurrency = 8)

export(task)
```

#### finalization_wait ####

Tasks have three active stages: ACTIVE, CLEANING and FINALIZING.  The ACTIVE stage is when
ordinary processes run.  This stage will last as long as processes are running and the
task is healthy.  The moment either all processes have finished successfully or the task
has reached a maximum process failure limit, it will go into CLEANING stage and send SIGTERMs
to all currently running processes and their process trees.  Once all processes have
terminated, the task goes into FINALIZING stage and invokes the schedule of all processes
with the "final" bit set.

This whole process from the end of ACTIVE stage to the end of FINALIZING must take place within
"finalization_wait" seconds.  If it does not complete within that time, all remaining
processes will be sent SIGKILLs (or if they depend upon processes that have not yet completed,
will never be invoked.)

Client applications with higher priority may be able to force a shorter
finalization wait (e.g. through parameters to `thermos kill`), so this is
mostly a best-effort signal.


## REPL ##

You can interactively experiment with the Thermos configuration REPL via the
`src/python/apache/thermos/config:repl` target:

```python
$ ./pants py src/python/apache/thermos/config:repl
Build operating on target: PythonBinary(src/python/apache/thermos/config/BUILD:repl)
Thermos Config REPL
>>> boilerplate = Process(cmdline = "echo hello world")
>>> boilerplate
Process(cmdline=echo hello world, daemon=0, max_failures=1, ephemeral=0, min_duration=5)
>>> boilerplate.check()
TypeCheck(FAILED): Process[name] is required.
>>> boilerplate(name = "hello world").check()
TypeCheck(OK)
```

## Thermos templating ##

The Thermos DSL is implemented in [pystachio](http://github.com/wickman/pystachio) which means that
a simple Mustache-like templating layer is available for use when configuring tasks.

### Ordinary templates ###

By using Mustache style templates in your job, you can do allow some amount of runtime configuration
of your tasks:

```
>>> process = Process(name = "hello", cmdline = "echo hello {{first}}")
>>> process
Process(cmdline=echo hello {{first}}, daemon=0, name=hello, max_failures=1, ephemeral=0, min_duration=5)
>>> process.check()
TypeCheck(FAILED): Process[cmdline] failed: Uninterpolated variables: {{first}}
```

This process leaves `{{first}}` as a free variable.  It can be filled elsewhere in the configuration, e.g. via
`%` or `bind`:
```
>>> process % {'first': 'brian'}
Process(cmdline=echo hello brian, daemon=0, name=hello, max_failures=1, ephemeral=0, min_duration=5)
>>> process.bind(first = 'brian')
Process(cmdline=echo hello brian, daemon=0, name=hello, max_failures=1, ephemeral=0, min_duration=5)
```

If this is left unbound, the thermos CLI will complain:

```
$ thermos run thermos/examples/tutorial/unbound.thermos
apache.thermos.config.loader.InvalidTask: Task[processes] failed: Element in ProcessList failed check: Process[cmdline] failed: Uninterpolated variables: {{first}}
```

But free variables can be populated at runtime using the `-E` parameter:

```
$ thermos run -E first=brian thermos/examples/tutorial/unbound.thermos
Writing log files to disk in /var/tmp
 INFO] Forking Process(hello)
 INFO] Process(hello) finished successfully [rc=0]
 INFO] Killing task id: unbound-20120530-124903.934384
 INFO]     => Current user: wickman
 INFO] Kill complete.
 INFO] Task succeeded.
```

### Special templates ###

Each Thermos task when run has a special `ThermosContext` template bound to the `thermos` variable.
Currently this provides three things: `thermos.task_id`, `thermos.user` and `thermos.ports`.  The
`task_id` is the id generated (or supplied at runtime, in the case of the thermos CLI) for the task
and the `thermos.user` is the real user the task runs as.  `thermos.ports` is a mapping of named ports
supplied by the user and exposed through the user interface.  For example, to run (and background)
the observer on port 1338:

```
$ thermos simplerun --daemon -P http:1338 'dist/thermos_observe.pex --http_port={{thermos.ports[http]}} --root=$HOME/.thermos'
```

If you go to http://localhost:1338, this bound port `http` will be exposed
via the UI on both the main and task pages.

To kill the background daemon:

```
$ thermos kill simple.*
```

### Includes ###

It is possible to include other Thermos configurations via the `include` parameter.  For example,
`thermos/examples/tutorial/lib/math.thermos`:

```python
bc = Process(cmdline = "echo 'scale={{precision}};{{command}}' | bc -l")
pi = bc(name = "pi").bind(command = "4*a(1)")
e  = bc(name = "e").bind(command = "e(1)")
```

and `thermos/examples/tutorial/pi.thermos`:

```python
include('lib/math.thermos')

export(Task(name = "compute_pi", processes = [pi]))
```

can then be executed with the free `precision` variable:

```shell
$ thermos run -E precision=500 thermos/examples/tutorial/pi.thermos
```
