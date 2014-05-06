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

These hooks will be registered two ways:
* Project hooks file. If a file named `AuroraHooks` is in the project directory
  where an aurora command is being executed, that file will be read,
  and its hooks will be registered.
* Configuration plugins. A configuration plugin can register hooks using an API.
  Hooks registered this way are, effectively, hardwired into the client executable.

The order of execution of hooks is unspecified: they may be called in
any order. There is no way to guarantee that one hook will execute
before some other hook.


### Global Hooks

Commands registered by the python call are called _global_ hooks,
because they will run for all configurations, whether or not they
specify any hooks in the configuration file.

In the implementation, hooks are registered in the module
`apache.aurora.client.cli.command_hooks`, using the class `GlobalCommandHookRegistry`.  A
global hook can be registered by calling `GlobalCommandHookRegistry.register_command_hook`
in a configuration plugin.

### Hook Files

A hook file is a file containing Python source code. It will be
dynamically loaded by the Aurora command line executable. After
loading, the client will check the module for a global variable named
"hooks", which contains a list of hook objects, which will be added to
the hook registry.

A project hooks file will be named `AuroraHooks`,
and will be located in either the directory where the command is being
executed, or one of its parent directories, up to the nearest git/mercurial
repository base.

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

## Skipping Hooks

In a perfect world, hooks would represent a global property or policy
that should always be enforced. Unfortunately, we don't live in a
perfect world, which means that sometimes, every rule needs to get
broken.

For example, an organization could decide that every configuration
must be checked in to source control before it could be
deployed. That's an entirely reasonable policy. It would be easy to
implement it using a hook. But what if there's a problem, and the
source repos is down?

The easiest solution is just to allow a user to add a `--skip-hooks`
flag to the command-line. But doing that means that an organization
can't actually use hooks to enforce policy, because users can skip
them whenever they want.

Instead, we'd like to have a system where it's possible to create
hooks to enforce policy, and then include a way of building policy
about when hooks can be skipped.

I'm using sudo as a rough model for this. Many organizations need to
give people the ability to run privileged commands, but they still
want to have some control. Sudo allows them to specify who is allowed
to run a privileged command; where they're allowed to run it; and what
command(s) they're allowed to run.  All of that is specified in a
special system file located in `/etc/sudoers` on a typical unix
machine.

In a world of distributed systems, this approach has one grave
weakness. An aurora client can be located on any machine that has
network access to a Mesos/Aurora cluster. It can be run by a user in
pretty much any way they want - from a machine they control, from a
special chroot they created, etc. Relying an a file being in a special
location on their machine isn't sufficient - it's too easy to
maliciously or erroneously run a command in an environment with an
invalid hooks exceptions file.

Instead, we've got two basic choices: hook exception rules can be
baked into the client executable, or they can be provided in a
network location.

### Specifying when hooks can be skipped

#### Hooks File

The module `apache.aurora.client.cli` contains a variable named
`GLOBAL_HOOK_SKIP_RULES_URL`. In the default distribution of Aurora, tihs variable contains
`None`. Users can modify this value for their local environments, providing
a site specific URL. If users attempt to bypass command hooks, and this
URL is not `None`, then the client will fetch the contents of that URL, and
attempt to interpret it as a hooks exception file.

The hooks exception file is written in JSON, with the following structure:

    { "rulename":
      {
      "hooks": [ "hook-name ", ... ],
      "users": [ string, ...],
      "commands": { "job": ["kill", "killall", ...], ... },
      "arg-patterns": [ "regexp-str", ... ]
      },
	  ...
    }

* `hooks` is a list of hook identifiers which can be skipped by a user
  that satisfies this rule. If omitted, then this rule applies to _all hooks_.
  (Omitting the `hooks` field is equivalent to giving it the value `['*']`.)
* `users` is a list of user names, or glob expressions that range over user
  names. This rule gives permission to those users to skip hooks. If omitted,
  then this rule allows _any user_ to skip hooks that satisfy the rest of this rule.
  Note that this is _user_ names, not
  _role_ names: the rules specify users that are allowed to skip commands.
  Some users that are allowed to work with a role account may be allowed to
  skip, while others cannot.
* `commands` is a map from nouns to lists of verbs. If a command `aurora n v`
  is being executed, this rule allows the hooks to be skipped if
  `v` is in `commands[n]`. If this is omitted, then it allows hooks to be skipped for all
  commands that satisfy the rest of the rule.
* `arg_patterns` is a list of glob patterns ranging over parameters.
  If any of the parameters of the command match the parameters in this list,
  the hook can be skipped. If ommitted, then this applies regardless of arguments.

For example, the following is a hook rules file which allows:
* The user "root" to skip any hook.
* Any user to skip hooks for test jobs.
* A specific group of users to skip hooks for jobs in cluster `east`
* Another group of users to skip hooks for `job kill` in cluster `west`.

    {
      "allow-admin": { "users": ["root"] },
	  "allow-test": { "users": ["\*"], "arg-patterns": ["\*/\*/test/\*"] },
	  "allow-east-users": { "users"=['john', 'mary', 'mike', 'sue'],
          "arg-patterns": ["east/\*/\*/\*"] },
	  "allow-west-kills": { "users": ["anne", "bill", "chris"],
          "commands": { "job": ["kill"]}, "arg-patterns" = ["west/\*/\*/\*"] }
    }

#### Programmatic Hooks Exceptions

The `GlobalHooksRegistry` contains the method `add_hooks_exception`, which allows
users to register local hooks exceptions using the `ConfigurationPlugin` mechanism.
A hooks exception object implements the following interface:

    class HooksException(object):
	  def allow_exception(self, hooks, role, noun, verb, args):
	    """Params:
        - hooks: a list of hook-names that the user wants to skip. If this
		  is ommitted, then this applies to all hooks.
		- role: the role requesting that hooks be skipped.
		- noun, verb: the noun and verb being executed.
		- the other command-line arguments.
		Returns: True if the user should be allowed to skip the requested hooks.
		"""
	    return False

When a user supplies the `--skip-hooks` argument, `allow_exception` is invoked on
each of the `HooksException` arguments. If _any_ of the hooks exception objects
returns `True`, then the user will be permitted to skip the hooks.

### Skipping Hooks

To skip a hook, a user uses a command-line option, `--skip-hooks`. The option can either
specify specific hooks to skip, or "all":

* `aurora --skip-hooks=all job create east/bozo/devel/myjob` will create a job
  without running any hooks.
* `aurora --skip-hooks=test,iq create east/bozo/devel/myjob` will create a job,
  and will skip only the hooks named "test" and "iq".


## Changes

4/30:
* Rule exceptions are defined in JSON, and they are specified to be loaded from
  a URL, not from a local file.
* Rule exceptions specify users, not roles.

4/27:
Major changes between this and the last version of this proposal.
* Command hooks can't be declared in a configuration file. There's a simple
  reason why: hooks run before a command's implementation is invoked.
  Config files are read during the commands invocation if necessary. If the
  hook is declared in the config file, by the time you know that it should
  have been run, it's too late. So I've removed config-hooks from the
  proposal. (API hooks defined by configs still work.)
* Skipping hooks. We expect aurora to be used inside of large
  organizations. One of the primary use-cases of hooks is to create
  enforcable policy that are specific to an organization. If hooks
  can just be skipped because a user wants to skip them, it means that
  the policy can't be enforced, which defeats the purpose of having them.
  So in this update, I propose a mechanism, loosely based on a `sudo`-like
  mechanism for defining when hooks can be skipped.
