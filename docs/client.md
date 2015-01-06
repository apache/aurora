Aurora Client
=============

Goals
-------

* A command line tool for interacting with Aurora that is easy for
  users to understand.
* A noun/verb command model.
* A modular source-code architecture.
* Non-disruptive transition for users.

Design
------

### Interface

In this section, we'll walk through the types of objects that the
client can manipulate, and the operations that need to be provided for
each object. These form the primary interface that engineers will use
to interact with Aurora.

In the command-line, each of the object types will have an Aurora
subcommand. The commands to manipulate the object type will follow the
type.

### The Job Noun

A job is a configured program ready to run in Aurora. A job is,
conceptually, a task factory: when a job is submitted to the Aurora
scheduler, it creates a collection of tasks. The job contains a
complete description of everything it needs to create a collection of
tasks. (Note that this subsumes "service" commands. A service is just
a task whose configuration sets the is_service flag, so we don't have
separate commands for working with services.) Jobs are specified using
`cluster/role/env/name` jobkey syntax.

* `aurora job create *jobkey* *config*`:  submits a job to a cluster, launching
  the task(s) specified by the job config.
* `aurora job status *jobkey*`: query job status. Prints information about the
  job, whether it's running, etc., to standard out. If jobkey includes globs,
  it should list all jobs that match the glob
* `aurora job kill *jobkey*/*instanceids*`: kill/stop some of a jobs instances.
  This stops a job' tasks; if the job has service tasks, they'll be  disabled,
  so that they won't restart.
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
  on the cluster will be listed; cluster/role, all jobs running on the cluster
  under the role will be listed, etc.

The Schedule Noun (Cron)
--------------------------

Cron is a scheduler adjunct that periodically runs a job on a
schedule. The cron commands all manipulate cron schedule entries. The
schedules are specified as a part of the job configuration.

* `aurora cron schedule jobkey config`: schedule a job to run by cron. If a cron
  job already exists replaces its template with a new one.
* `aurora cron deschedule jobkey`: removes a jobs entry from the cron schedule.
* `aurora cron status jobkey`: query for a scheduled job's status.

The Quota Noun
---------------

A quota is a data object maintained by the scheduler that specifies the maximum
resources that may be consumed by jobs owned by a particular role. In the future,
we may add new quota types. At some point, we'll also probably add an administrators
command to set quotas.

* `aurora quota get *cluster/role*`


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

Once the options are processed, the framework will start to execute the command.
Command execution consists of:

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
