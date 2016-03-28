# Command Hooks for the Aurora Client

## Introduction/Motivation

We've got hooks in the client that surround API calls. These are
pretty awkward, because they don't correlate with user actions. For
example, suppose we wanted a policy that said users weren't allowed to
kill all instances of a production job at once.

Right now, all that we could hook would be the "killJob" api call. But
kill (at least in newer versions of the client) normally runs in
batches. If a user called killall, what we would see on the API level
is a series of "killJob" calls, each of which specified a batch of
instances. We woudn't be able to distinguish between really killing
all instances of a job (which is forbidden under this policy), and
carefully killing in batches (which is permitted.) In each case, the
hook would just see a series of API calls, and couldn't find out what
the actual command being executed was!

For most policy enforcement, what we really want to be able to do is
look at and vet the commands that a user is performing, not the API
calls that the client uses to implement those commands.

So I propose that we add a new kind of hooks, which surround noun/verb
commands. A hook will register itself to handle a collection of (noun,
verb) pairs. Whenever any of those noun/verb commands are invoked, the
hooks methods will be called around the execution of the verb. A
pre-hook will have the ability to reject a command, preventing the
verb from being executed.

## Registering Hooks

These hooks will be registered via configuration plugins. A configuration plugin
can register hooks using an API. Hooks registered this way are, effectively,
hardwired into the client executable.

The order of execution of hooks is unspecified: they may be called in
any order. There is no way to guarantee that one hook will execute
before some other hook.


### Global Hooks

Commands registered by the python call are called _global_ hooks,
because they will run for all configurations, whether or not they
specify any hooks in the configuration file.

In the implementation, hooks are registered in the module
`apache.aurora.client.cli.command_hooks`, using the class
`GlobalCommandHookRegistry`. A global hook can be registered by calling
`GlobalCommandHookRegistry.register_command_hook` in a configuration plugin.

### The API

    class CommandHook(object)
      @property
      def name(self):
        """Returns a name for the hook."

      def get_nouns(self):
        """Return the nouns that have verbs that should invoke this hook."""

      def get_verbs(self, noun):
        """Return the verbs for a particular noun that should invoke his hook."""

      @abstractmethod
      def pre_command(self, noun, verb, context, commandline):
        """Execute a hook before invoking a verb.
        * noun: the noun being invoked.
        * verb: the verb being invoked.
        * context: the context object that will be used to invoke the verb.
          The options object will be initialized before calling the hook
        * commandline: the original argv collection used to invoke the client.
        Returns: True if the command should be allowed to proceed; False if the command
        should be rejected.
        """

      def post_command(self, noun, verb, context, commandline, result):
        """Execute a hook after invoking a verb.
        * noun: the noun being invoked.
        * verb: the verb being invoked.
        * context: the context object that will be used to invoke the verb.
          The options object will be initialized before calling the hook
        * commandline: the original argv collection used to invoke the client.
        * result: the result code returned by the verb.
        Returns: nothing
        """

    class GlobalCommandHookRegistry(object):
      @classmethod
      def register_command_hook(self, hook):
        pass

### Skipping Hooks

To skip a hook, a user uses a command-line option, `--skip-hooks`. The option can either
specify specific hooks to skip, or "all":

* `aurora --skip-hooks=all job create east/bozo/devel/myjob` will create a job
  without running any hooks.
* `aurora --skip-hooks=test,iq create east/bozo/devel/myjob` will create a job,
  and will skip only the hooks named "test" and "iq".
