Aurora Configuration Best Practices
===================================

Use As Few .aurora Files As Possible
------------------------------------

When creating your `.aurora` configuration, try to keep all versions of
a particular job within the same `.aurora` file. For example, if you
have separate jobs for `cluster1`, `cluster1` staging, `cluster1`
testing, and`cluster2`, keep them as close together as possible.

Constructs shared across multiple jobs owned by your team (e.g.
team-level defaults or structural templates) can be split into separate
`.aurora`files and included via the `include` directive.


Avoid Boilerplate
------------------

If you see repetition or find yourself copy and pasting any parts of
your configuration, it's likely an opportunity for templating. Take the
example below:

`redundant.aurora` contains:

    download = Process(
      name = 'download',
      cmdline = 'wget http://www.python.org/ftp/python/2.7.3/Python-2.7.3.tar.bz2',
      max_failures = 5,
      min_duration = 1)

    unpack = Process(
      name = 'unpack',
      cmdline = 'rm -rf Python-2.7.3 && tar xzf Python-2.7.3.tar.bz2',
      max_failures = 5,
      min_duration = 1)

    build = Process(
      name = 'build',
      cmdline = 'pushd Python-2.7.3 && ./configure && make && popd',
      max_failures = 1)

    email = Process(
      name = 'email',
      cmdline = 'echo Success | mail feynman@tmc.com',
      max_failures = 5,
      min_duration = 1)

    build_python = Task(
      name = 'build_python',
      processes = [download, unpack, build, email],
      constraints = [Constraint(order = ['download', 'unpack', 'build', 'email'])])

As you'll notice, there's a lot of repetition in the `Process`
definitions. For example, almost every process sets a `max_failures`
limit to 5 and a `min_duration` to 1. This is an opportunity for factoring
into a common process template.

Furthermore, the Python version is repeated everywhere. This can be
bound via structural templating as described in the [Advanced Binding](configuration-templating.md#AdvancedBinding)
section.

`less_redundant.aurora` contains:

    class Python(Struct):
      version = Required(String)
      base = Default(String, 'Python-{{version}}')
      package = Default(String, '{{base}}.tar.bz2')

    ReliableProcess = Process(
      max_failures = 5,
      min_duration = 1)

    download = ReliableProcess(
      name = 'download',
      cmdline = 'wget http://www.python.org/ftp/python/{{python.version}}/{{python.package}}')

    unpack = ReliableProcess(
      name = 'unpack',
      cmdline = 'rm -rf {{python.base}} && tar xzf {{python.package}}')

    build = ReliableProcess(
      name = 'build',
      cmdline = 'pushd {{python.base}} && ./configure && make && popd',
      max_failures = 1)

    email = ReliableProcess(
      name = 'email',
      cmdline = 'echo Success | mail {{role}}@foocorp.com')

    build_python = SequentialTask(
      name = 'build_python',
      processes = [download, unpack, build, email]).bind(python = Python(version = "2.7.3"))


Thermos Uses bash, But Thermos Is Not bash
-------------------------------------------

#### Bad

Many tiny Processes makes for harder to manage configurations.

    copy = Process(
      name = 'copy',
      cmdline = 'rcp user@my_machine:my_application .'
     )

     unpack = Process(
       name = 'unpack',
       cmdline = 'unzip app.zip'
     )

     remove = Process(
       name = 'remove',
       cmdline = 'rm -f app.zip'
     )

     run = Process(
       name = 'app',
       cmdline = 'java -jar app.jar'
     )

     run_task = Task(
       processes = [copy, unpack, remove, run],
       constraints = order(copy, unpack, remove, run)
     )

#### Good

Each `cmdline` runs in a bash subshell, so you have the full power of
bash. Chaining commands with `&&` or `||` is almost always the right
thing to do.

Also for Tasks that are simply a list of processes that run one after
another, consider using the `SequentialTask` helper which applies a
linear ordering constraint for you.

    stage = Process(
      name = 'stage',
      cmdline = 'rcp user@my_machine:my_application . && unzip app.zip && rm -f app.zip')

    run = Process(name = 'app', cmdline = 'java -jar app.jar')

    run_task = SequentialTask(processes = [stage, run])


Rarely Use Functions In Your Configurations
-------------------------------------------

90% of the time you define a function in a `.aurora` file, you're
probably Doing It Wrong(TM).

#### Bad

    def get_my_task(name, user, cpu, ram, disk):
      return Task(
        name = name,
        user = user,
        processes = [STAGE_PROCESS, RUN_PROCESS],
        constraints = order(STAGE_PROCESS, RUN_PROCESS),
        resources = Resources(cpu = cpu, ram = ram, disk = disk)
     )

     task_one = get_my_task('task_one', 'feynman', 1.0, 32*MB, 1*GB)
     task_two = get_my_task('task_two', 'feynman', 2.0, 64*MB, 1*GB)

#### Good

This one is more idiomatic. Forced keyword arguments prevents accidents,
e.g. constructing a task with "32*MB" when you mean 32MB of ram and not
disk. Less proliferation of task-construction techniques means
easier-to-read, quicker-to-understand, and a more composable
configuration.

    TASK_TEMPLATE = SequentialTask(
      user = 'wickman',
      processes = [STAGE_PROCESS, RUN_PROCESS],
    )

    task_one = TASK_TEMPLATE(
      name = 'task_one',
      resources = Resources(cpu = 1.0, ram = 32*MB, disk = 1*GB) )

    task_two = TASK_TEMPLATE(
      name = 'task_two',
      resources = Resources(cpu = 2.0, ram = 64*MB, disk = 1*GB)
    )
