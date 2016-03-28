Aurora Configuration Templating
===============================

The `.aurora` file format is just Python. However, `Job`, `Task`,
`Process`, and other classes are defined by a templating library called
*Pystachio*, a powerful tool for configuration specification and reuse.

[Aurora Configuration Reference](configuration.md)
has a full reference of all Aurora/Thermos defined Pystachio objects.

When writing your `.aurora` file, you may use any Pystachio datatypes, as
well as any objects shown in the *Aurora+Thermos Configuration
Reference* without `import` statements - the Aurora config loader
injects them automatically. Other than that the `.aurora` format
works like any other Python script.


Templating 1: Binding in Pystachio
----------------------------------

Pystachio uses the visually distinctive {{}} to indicate template
variables. These are often called "mustache variables" after the
similarly appearing variables in the Mustache templating system and
because the curly braces resemble mustaches.

If you are familiar with the Mustache system, templates in Pystachio
have significant differences. They have no nesting, joining, or
inheritance semantics. On the other hand, when evaluated, templates
are evaluated iteratively, so this affords some level of indirection.

Let's start with the simplest template; text with one
variable, in this case `name`;

    Hello {{name}}

If we evaluate this as is, we'd get back:

    Hello

If a template variable doesn't have a value, when evaluated it's
replaced with nothing. If we add a binding to give it a value:

    { "name" : "Tom" }

We'd get back:

    Hello Tom

Every Pystachio object has an associated `.bind` method that can bind
values to {{}} variables. Bindings are not immediately evaluated.
Instead, they are evaluated only when the interpolated value of the
object is necessary, e.g. for performing equality or serializing a
message over the wire.

Objects with and without mustache templated variables behave
differently:

    >>> Float(1.5)
    Float(1.5)

    >>> Float('{{x}}.5')
    Float({{x}}.5)

    >>> Float('{{x}}.5').bind(x = 1)
    Float(1.5)

    >>> Float('{{x}}.5').bind(x = 1) == Float(1.5)
    True

    >>> contextual_object = String('{{metavar{{number}}}}').bind(
    ... metavar1 = "first", metavar2 = "second")

    >>> contextual_object
    String({{metavar{{number}}}})

    >>> contextual_object.bind(number = 1)
    String(first)

    >>> contextual_object.bind(number = 2)
    String(second)

You usually bind simple key to value pairs, but you can also bind three
other objects: lists, dictionaries, and structurals. These will be
described in detail later.


### Structurals in Pystachio / Aurora

Most Aurora/Thermos users don't ever (knowingly) interact with `String`,
`Float`, or `Integer` Pystashio objects directly. Instead they interact
with derived structural (`Struct`) objects that are collections of
fundamental and structural objects. The structural object components are
called *attributes*. Aurora's most used structural objects are `Job`,
`Task`, and `Process`:

    class Process(Struct):
      cmdline = Required(String)
      name = Required(String)
      max_failures = Default(Integer, 1)
      daemon = Default(Boolean, False)
      ephemeral = Default(Boolean, False)
      min_duration = Default(Integer, 5)
      final = Default(Boolean, False)

Construct default objects by following the object's type with (). If you
want an attribute to have a value different from its default, include
the attribute name and value inside the parentheses.

    >>> Process()
    Process(daemon=False, max_failures=1, ephemeral=False,
      min_duration=5, final=False)

Attribute values can be template variables, which then receive specific
values when creating the object.

    >>> Process(cmdline = 'echo {{message}}')
    Process(daemon=False, max_failures=1, ephemeral=False, min_duration=5,
            cmdline=echo {{message}}, final=False)

    >>> Process(cmdline = 'echo {{message}}').bind(message = 'hello world')
    Process(daemon=False, max_failures=1, ephemeral=False, min_duration=5,
            cmdline=echo hello world, final=False)

A powerful binding property is that all of an object's children inherit its
bindings:

    >>> List(Process)([
    ... Process(name = '{{prefix}}_one'),
    ... Process(name = '{{prefix}}_two')
    ... ]).bind(prefix = 'hello')
    ProcessList(
      Process(daemon=False, name=hello_one, max_failures=1, ephemeral=False, min_duration=5, final=False),
      Process(daemon=False, name=hello_two, max_failures=1, ephemeral=False, min_duration=5, final=False)
      )

Remember that an Aurora Job contains Tasks which contain Processes. A
Job level binding is inherited by its Tasks and all their Processes.
Similarly a Task level binding is available to that Task and its
Processes but is *not* visible at the Job level (inheritance is a
one-way street.)

#### Mustaches Within Structurals

When you define a `Struct` schema, one powerful, but confusing, feature
is that all of that structure's attributes are Mustache variables within
the enclosing scope *once they have been populated*.

For example, when `Process` is defined above, all its attributes such as
{{`name`}}, {{`cmdline`}}, {{`max_failures`}} etc., are all immediately
defined as Mustache variables, implicitly bound into the `Process`, and
inherit all child objects once they are defined.

Thus, you can do the following:

    >>> Process(name = "installer", cmdline = "echo {{name}} is running")
    Process(daemon=False, name=installer, max_failures=1, ephemeral=False, min_duration=5,
            cmdline=echo installer is running, final=False)

WARNING: This binding only takes place in one direction. For example,
the following does NOT work and does not set the `Process` `name`
attribute's value.

    >>> Process().bind(name = "installer")
    Process(daemon=False, max_failures=1, ephemeral=False, min_duration=5, final=False)

The following is also not possible and results in an infinite loop that
attempts to resolve `Process.name`.

    >>> Process(name = '{{name}}').bind(name = 'installer')

Do not confuse Structural attributes with bound Mustache variables.
Attributes are implicitly converted to Mustache variables but not vice
versa.

### Templating 2: Structurals Are Factories

#### A Second Way of Templating

A second templating method is both as powerful as the aforementioned and
often confused with it. This method is due to automatic conversion of
Struct attributes to Mustache variables as described above.

Suppose you create a Process object:

    >>> p = Process(name = "process_one", cmdline = "echo hello world")

    >>> p
    Process(daemon=False, name=process_one, max_failures=1, ephemeral=False, min_duration=5,
            cmdline=echo hello world, final=False)

This `Process` object, "`p`", can be used wherever a `Process` object is
needed. It can also be reused by changing the value(s) of its
attribute(s). Here we change its `name` attribute from `process_one` to
`process_two`.

    >>> p(name = "process_two")
    Process(daemon=False, name=process_two, max_failures=1, ephemeral=False, min_duration=5,
            cmdline=echo hello world, final=False)

Template creation is a common use for this technique:

    >>> Daemon = Process(daemon = True)
    >>> logrotate = Daemon(name = 'logrotate', cmdline = './logrotate conf/logrotate.conf')
    >>> mysql = Daemon(name = 'mysql', cmdline = 'bin/mysqld --safe-mode')

### Advanced Binding

As described above, `.bind()` binds simple strings or numbers to
Mustache variables. In addition to Structural types formed by combining
atomic types, Pystachio has two container types; `List` and `Map` which
can also be bound via `.bind()`.

#### Bind Syntax

The `bind()` function can take Python dictionaries or `kwargs`
interchangeably (when "`kwargs`" is in a function definition, `kwargs`
receives a Python dictionary containing all keyword arguments after the
formal parameter list).

    >>> String('{{foo}}').bind(foo = 'bar') == String('{{foo}}').bind({'foo': 'bar'})
    True

Bindings done "closer" to the object in question take precedence:

    >>> p = Process(name = '{{context}}_process')
    >>> t = Task().bind(context = 'global')
    >>> t(processes = [p, p.bind(context = 'local')])
    Task(processes=ProcessList(
      Process(daemon=False, name=global_process, max_failures=1, ephemeral=False, final=False,
              min_duration=5),
      Process(daemon=False, name=local_process, max_failures=1, ephemeral=False, final=False,
              min_duration=5)
    ))

#### Binding Complex Objects

##### Lists

    >>> fibonacci = List(Integer)([1, 1, 2, 3, 5, 8, 13])
    >>> String('{{fib[4]}}').bind(fib = fibonacci)
    String(5)

##### Maps

    >>> first_names = Map(String, String)({'Kent': 'Clark', 'Wayne': 'Bruce', 'Prince': 'Diana'})
    >>> String('{{first[Kent]}}').bind(first = first_names)
    String(Clark)

##### Structurals

    >>> String('{{p.cmdline}}').bind(p = Process(cmdline = "echo hello world"))
    String(echo hello world)

### Structural Binding

Use structural templates when binding more than two or three individual
values at the Job or Task level. For fewer than two or three, standard
key to string binding is sufficient.

Structural binding is a very powerful pattern and is most useful in
Aurora/Thermos for doing Structural configuration. For example, you can
define a job profile. The following profile uses `HDFS`, the Hadoop
Distributed File System, to designate a file's location. `HDFS` does
not come with Aurora, so you'll need to either install it separately
or change the way the dataset is designated.

    class Profile(Struct):
      version = Required(String)
      environment = Required(String)
      dataset = Default(String, hdfs://home/aurora/data/{{environment}}')

    PRODUCTION = Profile(version = 'live', environment = 'prod')
    DEVEL = Profile(version = 'latest',
                    environment = 'devel',
                    dataset = 'hdfs://home/aurora/data/test')
    TEST = Profile(version = 'latest', environment = 'test')

    JOB_TEMPLATE = Job(
      name = 'application',
      role = 'myteam',
      cluster = 'cluster1',
      environment = '{{profile.environment}}',
      task = SequentialTask(
        name = 'task',
        resources = Resources(cpu = 2, ram = 4*GB, disk = 8*GB),
        processes = [
      Process(name = 'main', cmdline = 'java -jar application.jar -hdfsPath
                 {{profile.dataset}}')
        ]
       )
     )

    jobs = [
      JOB_TEMPLATE(instances = 100).bind(profile = PRODUCTION),
      JOB_TEMPLATE.bind(profile = DEVEL),
      JOB_TEMPLATE.bind(profile = TEST),
     ]

In this case, a custom structural "Profile" is created to self-document
the configuration to some degree. This also allows some schema
"type-checking", and for default self-substitution, e.g. in
`Profile.dataset` above.

So rather than a `.bind()` with a half-dozen substituted variables, you
can bind a single object that has sensible defaults stored in a single
place.
