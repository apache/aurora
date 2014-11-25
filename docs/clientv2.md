Aurora Client v2
=================

Overview
-----------

Our goal is to replace the current Aurora command-line client. The
current client suffers from an early monolithic structure, and a long
development history of rapid unplanned evolution.

In addition to its internal problems, the current Aurora client is
confusing for users. There are several different kinds of objects
manipulated by the Aurora command line, and the difference between
them is often not clear. (What's the difference between a job and a
configuration?) For each type of object, there are different commands,
and it's hard to remember which command should be used for which kind
of object.

Instead of continuing to let the Aurora client develop and evolve
randomly, it's time to take a principled look at the Aurora command
line, and figure out how to make our command line processing make
sense. At the same time, the code needs to be cleaned up, and divided
into small comprehensible units based on a plugin architecture.

Doing this now will give us a more intuitive, consistent, and easy to
use client, as well as a sound platform for future development.

Goals
-------

* A command line tool for interacting with Aurora that is easy for
  users to understand.
* A noun/verb command model.
* A modular source-code architecture.
* Non-disruptive transition for users.

Non-Goals
----------

* The most important non-goal is that we're not trying to redesign the
  Aurora scheduler, the Aurora executor, or any of the peripheral tools
  that the Aurora command line interacts with; we only want to create a
  better command line client.
* We do not want to change thermos, mesos, hadoop, etc.
* We do not want to create new objects that users will work with to
  interact with Mesos or Aurora.
* We do not want to change Aurora job configuration files or file formats.
* We do not want to change the Aurora API.
* We don't want to boil the ocean: there are many things that we could
  include in the scope of this project, but we don't want to be
  distracted by re-implementing all of twitter.commons in order to
  create a perfect Aurora client.


Background
-----------

Aurora is a system that's used to run and manage services and
service-like jobs running in a datacenter. Aurora takes care of
allocating resources in order to schedule and run jobs without
requiring teams to manage dedicated hardware. The heart of Aurora is
called the scheduler, and is responsible for finding and assigning
resources to tasks.

The Aurora scheduler provides a thrift API. The scheduler API is
low-level and difficult to interact with. Users do not interact
directly with the Aurora API; instead, they use a command-line tool,
which provides a collection of easy-to-use commands. This command-line
tool, in turn, talks to the scheduler API to launch and manage jobs in
datacenter clusters. The command-line tool is called the Aurora
client.

The current implementation of the Aurora client is haphazard,
and really needs to be cleaned up:

- The code is monolithic and hard to maintain. It's implemented using
  `twitter.common.app`, which assumes that all of the command code lives
  in a single source file. To work around this, and allow some
  subdivision, it uses a hack of `twitter.common.app` to force
  registration of commands from multiple modules. It's hard to
  understand, and hard to modify.
- The current code is very difficult to test. Because of the way it's
  built, there is no consistent way of passing key application data
  around. As a result, each unit test of client operations needs a
  difficult-to-assemble custom setup of mock objects.
- The current code handles errors poorly, and it is difficult to
  fix. Many common errors produce unacceptable results. For example,
  issuing an unknown command generates an error message "main takes 0
  parameters but received 1"; passing an invalid parameter to other
  commands frequently produces a stack trace.
- The current command line is confusing for users. There are several
  different kinds of objects manipulated by the Aurora command line,
  and the difference between them is often not entirely clear. (What's
  the difference between a job and a configuration?)
  For each type of object, there are different
  commands, and it's frequently not clear just which command should be
  used for which object.


Instead of continuing to let it develop and evolve randomly, it's time
to take a principled look at the Aurora command line, and figure out
how to make command line processing make sense. At the same time, the
code needs to be cleaned up, and divided into small comprehensible
units based on a plugin architecture.

Requirements
-------------

Aurora is aimed at engineers who run jobs and services in a
datacenter. As a result, the requirements for the aurora client are
all engineering focused:

* __Consistency__: commands should follow a consistent structure, so that
  users can apply knowledge and intuition gained from working with
  some aurora commands to new commands. This means that when commands
  can re-use the same options, they should; that objects should be
  referred to by consistent syntax throughout the tool.
* __Helpfulness__: commands should be structured so that the system can
  generate helpful error messages. If a user just runs "aurora", they
  should get a basic usage message. If they try to run an invalid
  command, they should get a message that the command is invalid, not
  a stack dump or "command main() takes 0 parameters but received
  2". Commands should not generate extraneous output that obscures the
  key facts that the user needs to know, and the default behavior of
  commands should not generate outputs that will be routinely ignored
  by users.
* __Extensibility__: it should be easy to plug in new commands,
  including custom commands, to adapt the Aurora client to new
  environments.
* __Script-friendly command output__: every command should at least include
  an option that generates output that's script-friendly. Scripts should be
  able to work with command-output without needing to do screen scraping.
* __Scalability__: the tools should be usable for any foreseeable size
  of Aurora datacenters and machine clusters.

Design Overview
-----------------

The Aurora client will be reimplemented using a noun-verb model,
similar to the cmdlet model used by Monad/Windows Powershell. Users
will work by providing a noun for the type of object being operated
on, and a verb for the specific operation being performed on the
object, followed by parameters. For example, to create a job, the user
would execute: "`aurora job create smfd/mchucarroll/devel/jobname
job.aurora`". The noun is `job` and the verb is `create`.

The client will be implemented following that noun-verb
convention. Each noun will be a separate component, which can be
registered into the command-line framework. Each verb will be
implemented by a class that registers with the appropriate noun. Nouns
and verbs will each provide methods that add their command line
options and parameters to the options parser, using the Python
argparse library.

Detailed Design
-----------------

### Interface

In this section, we'll walk through the types of objects that the
client can manipulate, and the operations that need to be provided for
each object. These form the primary interface that engineers will use
to interact with Aurora.

In the command-line, each of the object types will have an Aurora
subcommand. The commands to manipulate the object type will follow the
type. For example, here are several commands in the old syntax
contrasted against the new noun/verb syntax.

* Get quota for a role:
   * Noun/Verb syntax:  `aurora quota get west/www-data`
   * Old syntax: `aurora get_quota --cluster=smf1 www-data`
* Create job:
   * Noun/Verb syntax: `aurora job create west/www-data/test/job job.aurora`
   * Old syntax: `aurora create west/www-data/test/job job.aurora`
* Schedule a job to run at a specific interval:
   * Noun/verb: `aurora cron schedule east/www-data/test/job job.aurora`
   * Old: `aurora create east/www-data/test/job job.aurora`

As you can see in these examples, the new syntax is more consistent:
you always specify the cluster where a command executes as part of an
identifier, where in the old syntax, it was sometimes part of the
jobkey and sometimes specified with a "--cluster" option.

The new syntax is also more clear and explicit: even without knowing
much about Aurora, it's clear what objects each command is acting on,
where in the old syntax, commands like "create" are unclear.

### The Job Noun

A job is a configured program ready to run in Aurora. A job is,
conceptually, a task factory: when a job is submitted to the Aurora
scheduler, it creates a collection of tasks. The job contains a
complete description of everything it needs to create a collection of
tasks. (Note that this subsumes "service" commands. A service is just
a task whose configuration sets the is_service flag, so we don't have
separate commands for working with services.) Jobs are specified using
`cluster/role/env/name` jobkey syntax.

* `aurora job create *jobkey* *config*`:  submits a job to a cluster, launching the task(s) specified by the job config.
* `aurora job status *jobkey*`: query job status. Prints information about the job,
  whether it's running, etc., to standard out. If jobkey includes
  globs, it should list all jobs that match the glob
* `aurora job kill *jobkey*/*instanceids*`: kill/stop some of a jobs instances. This stops a job' tasks; if the job
  has service tasks, they'll be  disabled, so that they won't restart.
* `aurora job killall *jobkey*`: kill all of the instances of a job. This
  is distinct from the *kill* command as a safety measure: omitting the
  instances from a kill command shouldn't result in destroying the entire job.
* `aurora job restart *jobkey*`: conceptually, this will kill a job, and then
  launch it again. If the job does not exist, then fail with an error
  message.  In fact, the underlying implementation does the
  kill/relaunch on a rolling basis - so it's not an immediate kill of
  all shards/instances, followed by a delay as all instances relaunch,
  but rather a controlled gradual process.
* `aurora job list *jobkey*`: list all jobs that match the jobkey spec that are
  registered with the scheduler. This will include both jobs that are
  currently running, and jobs that are scheduled to run at a later
  time. The job key can be partial: if it specifies cluster, all jobs
  on the cluster will be listed; cluster/role, all jobs running on the cluster under the role will be listed, etc.

The Schedule Noun (Cron)
--------------------------

Note (3/21/2014): The "cron" noun is _not_ implemented yet.

Cron is a scheduler adjunct that periodically runs a job on a
schedule. The cron commands all manipulate cron schedule entries. The
schedules are specified as a part of the job configuration.

* `aurora cron schedule jobkey config`: schedule a job to run by cron. If a cron job already exists
replaces its template with a new one.
* `aurora cron deschedule jobkey`: removes a jobs entry from the cron schedule.
* `aurora cron status jobkey`: query for a scheduled job's status.

The Quota Noun
---------------

A quota is a data object maintained by the scheduler that specifies the maximum
resources that may be consumed by jobs owned by a particular role. In the future,
we may add new quota types. At some point, we'll also probably add an administrators
command to set quotas.

* `aurora quota get *cluster/role*`


Implementation
---------------

The current command line is monolithic. Every command on an Aurora
object is a top-level command in the Aurora client. In the
restructured command line, each of the primary object types
manipulated by Aurora should have its own sub-command.

* Advantages of this approach:
   * Easier to detangle the command-line processing. The top-level
     command-processing will be a small set of subcommand
     processors. Option processing for each subcommand can be offloaded
     to a separate module.
   * The aurora top-level help command will be much more
     comprehensible. Instead of giving a huge list of every possible
     command, it will present the list of top-level object types, and
     then users can request help on the commands for a specific type
     of object.
   * The sub-commands can be separated into distinct command-line
     tools when appropriate.

### Command Structure and Options Processing

The implementation will follow closely on Pants goals. Pants goals use
a static registration system to add new subcommands. In pants, each
goal command is an implementation of a command interface, and provides
implementations of methods to register options and parameters, and to
actually execute the command. In this design, commands are modular and
easy to implement, debug, and combine in different ways.

For the Aurora client, we plan to use a two-level variation of the
basic concept from pants. At the top-level we will have nouns. A noun
will define some common command-line parameters required by all of its
verbs, and will provide a registration hook for attaching verbs. Nouns
will be implemented as a subclass of a basic Noun type.

Each verb will, similarly, be implemented as a subclass of Verb. Verbs
will be able to specify command-line options and parameters.

Both `Noun` and `Verb` will be subclasses of a common base-class `AuroraCommand`:

    class AuroraCommand(object):
      def get_options(self):
      """Gets the set of command-line options objects for this command.
      The result is a list of CommandOption objects.
       """
        pass

      @property
      def help(self):
        """Returns the help message for this command"""

      @property
      def usage(self):
        """Returns a short usage description of the command"""

      @property
      def name(self):
        """Returns the command name"""


A command-line tool will be implemented as an instance of a `CommandLine`:

    class CommandLine(object):
      """The top-level object implementing a command-line application."""

      @property
      def name(self):
        """Returns the name of this command-line tool"""

      def print_out(self, str):
        print(str)

      def print_err(self, str):
        print(str, file=sys.stderr)

      def register_noun(self, noun):
        """Adds a noun to the application"""

      def register_plugin(self, plugin):
	     """Adds a configuration plugin to the system"""


Nouns are registered into a command-line using the `register_noun`
method. They are weakly coupled to the application, making it easy to
use a single noun in several different command-line tools. Nouns allow
the registration of verbs using the `register_verb` method.

When commands execute, they're given an instance of a *context object*.
The context object must be an instance of a subclass of `AuroraCommandContext`.
Options, parameters, and IO are all accessed using the context object. The context
is created dynamically by the noun object owning the verb being executed. Developers
are strongly encouraged to implement custom contexts for their nouns, and move functionality
shared by the noun's verbs into the context object. The context interface is:

    class Context(object):
      class Error(Exception): pass

      class ArgumentException(Error): pass

      class CommandError(Error):

      @classmethod
      def exit(cls, code, msg):
	    """Exit the application with an error message"""
        raise cls.CommandError(code, msg)

     def print_out(self, msg, indent=0):
       """Prints a message to standard out, with an indent"""

     def print_err(self, msg, indent=0):
       """Prints a message to standard err, with an indent"""


In addition to nouns and verbs, there's one more kind of registerable
component, called a *configuration plugin*. These objects add a set of
command-line options that can be passed to *all* of the commands
implemented in the tool. Before the command is executed, the
configuration plugin will be invoked, and will process its
command-line arguments. This is useful for general configuration
changes, like establish a secure tunnel to talk to machines in a
datacenter. (A useful way to think of a plugin is as something like an
aspect that can be woven in to aurora to provide environment-specific
configuration.) A configuration plugin is implemented as an instance
of class `ConfigurationPlugin`, and registered with the
`register_plugin` method of the `CommandLine` object. The interface of
a plugin is:

    class ConfigurationPlugin(object):
      """A component that can be plugged in to a command-line."""

      @abstractmethod
      def get_options(self):
        """Return the set of options processed by this plugin"""

      @abstractmethod
      def execute(self, context):
        """Run the context/command line initialization code for this plugin."""


### Command Execution

The options process and command execution is built as a facade over Python's
standard argparse. All of the actual argument processing is done by the
argparse library.

Once the options are processed, the framework will start to execute the command. Command execution consists of:

# Create a context object. The framework will use the argparse options to identify
  which noun is being invoked, and will call that noun's `create_context` method.
  The argparse options object will be stored in the context.
# Execute any configuration plugins. Before any command is invoked, the framework
  will first iterate over all of the registered configuration plugins. For each
  plugin, it will invoke the `execute` method.
# The noun will use the context to find out what verb is being invoked, and it will
  then call that verb's `execute` method.
# The command will exit. Its return code will be whatever was returned by the verb's
  `execute` method.

Commands are expected to return a code from a list of standard exit codes,
which can be found in `src/main/python/apache/aurora/client/cli/__init__.py`.
