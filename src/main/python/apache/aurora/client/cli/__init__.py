#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

'''Command-line tooling infrastructure for aurora client v2.

This module provides a framework for a noun/verb command-line application.
In this framework, an application is structured around a collection of basic objects (nouns)
that can be manipulated by the command line, where each type of object provides a collection
of operations (verbs). Every command invocation consists of the name of the noun, followed by
one of the verbs for that noun, followed by other arguments needed by the verb.

For example:
- To create a job, the noun is "job", the verb is "create":
  $ aurora job create us-west/www/prod/server server.aurora
- To find out the resource quota for a specific user, the noun is "user" and the verb is
  "get_quota":
  $ aurora user get_quota mchucarroll
'''

from __future__ import print_function

import argparse
import getpass
import logging
import sys
from abc import abstractmethod
from uuid import uuid1

from twitter.common.python.pex import PexInfo

from .command_hooks import GlobalCommandHookRegistry
from .logsetup import TRANSCRIPT
from .options import CommandOption

# Constants for standard return codes.
EXIT_OK = 0
EXIT_INTERRUPTED = 130
EXIT_INVALID_CONFIGURATION = 3
EXIT_COMMAND_FAILURE = 4
EXIT_INVALID_COMMAND = 5
EXIT_INVALID_PARAMETER = 6
EXIT_NETWORK_ERROR = 7
EXIT_PERMISSION_VIOLATION = 8
EXIT_TIMEOUT = 9
EXIT_API_ERROR = 10
EXIT_UNKNOWN_ERROR = 20


# Set up a logging call that adds a unique identifier for this invocation
# of the client. Log messages sent via this call will contain two additional
# fields in the log record: "clientid", which contains a UUID for the client
# invocation, and "user", which contains the username of the user who invoked
# the client.

LOGGER_NAME = "aurora_client"
logger = logging.getLogger(LOGGER_NAME)
CLIENT_ID = uuid1()


# A location where you can find a site-specific file containing
# global hook skip rules. This can be something like a link into a file stored in a git
# repos.
GLOBAL_HOOK_SKIP_RULES_URL = None


def print_aurora_log(sev, msg, *args, **kwargs):
  extra = kwargs.get("extra", {})
  extra["clientid"] = CLIENT_ID
  extra["user"] = getpass.getuser()
  extra["logger_name"] = LOGGER_NAME
  kwargs["extra"] = extra
  logger.log(sev, msg, *args, **kwargs)


def get_client_version():
  try:
    pexpath = sys.argv[0]
    pex_info = PexInfo.from_pex(pexpath)
    return ("%s@%s" % (pex_info.build_properties.get("sha", "unknown"),
        pex_info.build_properties.get("date", "unknown")))
  except (IOError, OSError):
    return "VersionUnknown"


class Context(object):
  class Error(Exception): pass
  class ArgumentException(Error): pass

  class CommandError(Error):
    def __init__(self, code, msg):
      super(Context.CommandError, self).__init__(msg)  # noqa:T800
      self.msg = msg
      self.code = code

  REVEAL_ERRORS = False

  def __init__(self):
    self.options = None
    self.logging_level = None

  @classmethod
  def reveal_errors(cls):
    return cls.REVEAL_ERRORS

  @classmethod
  def enable_reveal_errors(cls):
    cls.REVEAL_ERRORS = True

  @classmethod
  def disable_reveal_errors(cls):
    cls.REVEAL_ERRORS = False

  @classmethod
  def exit(cls, code, msg):
    raise cls.CommandError(code, msg)

  def set_options(self, options):
    """Add the options object to a context.
    This is separated from the constructor to make patching tests easier.
    """
    self.options = options

  def set_args(self, args):
    """Add the raw argument list to a context."""
    self.args = args

  def print_out(self, msg, indent=0):
    """Prints output to standard out with indent.
    For debugging purposes, it's nice to be able to patch this and capture output.
    """
    indent_str = " " * indent
    lines = msg.split("\n")
    for line in lines:
      print("%s%s" % (indent_str, line))

  def print_err(self, msg, indent=0):
    """Prints output to standard error, with an indent."""
    indent_str = " " * indent
    lines = msg.split("\n")
    for line in lines:
      print("%s%s" % (indent_str, line), file=sys.stderr)

  def print_log(self, severity, msg, *args, **kwargs):
    """Print a message to a log.
    Logging with this method is intended for generating output for aurora developers/maintainers.
    Log output isn't for users - information much more detailed than users want may be logged.
    Logs generated for clients of a cluster may be gathered in a centralized database by the
    aurora admins for that cluster.
    """
    print_aurora_log(severity, msg, *args, **kwargs)


class ConfigurationPlugin(object):
  """A component that can be plugged in to a command-line to add new configuration options.

  For example, if a production environment is protected behind some
  kind of gateway, a ConfigurationPlugin could be created that
  performs a security handshake with the gateway before any of the commands
  attempt to communicate with the environment.
  """

  class Error(Exception):
    def __init__(self, msg, code=0):
      super(ConfigurationPlugin.Error, self).__init__(msg)  # noqa:T800
      self.code = code
      self.msg = msg

  @abstractmethod
  def get_options(self):
    """Return the set of options processed by this plugin"""
    return []

  @abstractmethod
  def before_dispatch(self, raw_args):
    """Run some code before dispatching to the client.
    If a ConfigurationPlugin.Error exception is thrown, aborts the command execution.
    """
    return raw_args

  @abstractmethod
  def before_execution(self, context):
    """Run the context/command line initialization code for this plugin before
    invoking the verb.
    The before_execution method behaves as if it's part of the implementation of the
    verb being invoked. It has access to the same context that will be used by the command.
    Any errors that occur during the execution should be signalled using ConfigurationPlugin.Error.
    """

  @abstractmethod
  def after_execution(self, context, result_code):
    """Run cleanup code after the execution of the verb has completed.
    This code should *not* ever throw exceptions. It's just cleanup code, and it shouldn't
    ever change the result of invoking a verb. This can throw a ConfigurationPlugin.Error,
    which will generate log records, but will not otherwise effect the execution of the command.
    """


class AuroraCommand(object):
  def setup_options_parser(self, argparser):
    """Sets up command line options parsing for this command.
    This is a thin veneer over the standard python argparse system.
    :param argparser: the argument parser where this command can add its arguments.
    """
    pass

  def add_option(self, argparser, option):
    """Adds an option spec encapsulated an a CommandOption to this command's argument parser."""
    if not isinstance(option, CommandOption):
      raise TypeError('Command option object must be an instance of CommandOption')
    option.add_to_parser(argparser)

  @property
  def help(self):
    """Returns the help message for this command"""

  @property
  def usage(self):
    """Returns a short usage description of the command"""

  @property
  def name(self):
    """Returns the command name"""


class CommandLine(object):
  """The top-level object implementing a command-line application."""

  @property
  def name(self):
    """Returns the name of this command-line tool"""

  def print_out(self, s, indent=0):
    indent_str = " " * indent
    print("%s%s" % (indent_str, s))

  def print_err(self, s, indent=0):
    indent_str = " " * indent
    print("%s%s" % (indent_str, s), file=sys.stderr)

  def __init__(self):
    self.nouns = None
    self.parser = None
    self.plugins = []

  def register_noun(self, noun):
    """Adds a noun to the application"""
    if self.nouns is None:
      self.nouns = {}
    if not isinstance(noun, Noun):
      raise TypeError("register_noun requires a Noun argument")
    self.nouns[noun.name] = noun
    noun.set_commandline(self)

  def register_plugin(self, plugin):
    self.plugins.append(plugin)

  def setup_options_parser(self):
    """ Builds the options parsing for the application."""
    self.parser = argparse.ArgumentParser()
    subparser = self.parser.add_subparsers(dest="noun")
    for (name, noun) in self.nouns.items():
      noun_parser = subparser.add_parser(name, help=noun.help)
      noun.internal_setup_options_parser(noun_parser)

  def help_cmd(self, args):
    """Generates a help message for a help request.
    There are three kinds of help requests: a simple no-parameter request (help) which generates
    a list of all of the commands; a one-parameter (help noun) request, which generates the help
    for a particular noun, and a two-parameter request (help noun verb) which generates the help
    for a particular verb.
    """
    if args is None or len(args) == 0:
      self.print_out(self.composed_help)
    elif len(args) == 1:
      if args[0] in self.nouns:
        self.print_out(self.nouns[args[0]].composed_help)
        return EXIT_OK
      else:
        self.print_err('Unknown noun "%s"' % args[0])
        self.print_err("Valid nouns are: %s" % [k for k in self.nouns])
        return EXIT_INVALID_PARAMETER
    elif len(args) == 2:
      if args[0] in self.nouns:
        if args[1] in self.nouns[args[0]].verbs:
          self.print_out(self.nouns[args[0]].verbs[args[1]].composed_help)
          return EXIT_OK
        else:
          self.print_err('Noun "%s" does not support a verb "%s"' % (args[0], args[1]))
          verbs = [v for v in self.nouns[args[0]].verbs]
          self.print_err('Valid verbs for "%s" are: %s' % (args[0], verbs))
          return EXIT_INVALID_PARAMETER
      else:
        self.print_err("Unknown noun %s" % args[0])
        return EXIT_INVALID_PARAMETER
    else:
      self.print_err("Unknown help command: %s" % (" ".join(args)))
      self.print_err(self.composed_help)
      return EXIT_INVALID_PARAMETER

  @property
  def composed_help(self):
    """Get a fully composed, well-formatted help message"""
    result = ["Aurora Client version %s" % get_client_version(), "Usage:"]
    for noun in self.registered_nouns:
      result += ["==Commands for %ss" % noun]
      result += ["  %s" % s for s in self.nouns[noun].usage] + [""]
    result.append("\nRun 'help noun' or 'help noun verb' for help about a specific command")
    return "\n".join(result)

  def register_nouns(self):
    """This method should overridden by applications to register the collection of nouns
    that they can manipulate.

    Noun registration is done on-demand, when either get_nouns or execute is called.
    This allows the command-line tool a small amount of self-customizability depending
    on the environment in which it is being used.

    For example, if a cluster is being run via AWS, then you could provide an
    AWS noun with a set of operations for querying AWS status, billing stats,
    etc. You wouldn't want to clutter the help output with AWS commands for users
    that weren't using AWS. So you could have the command-line check the cluster.json
    file, and only register the AWS noun if there was an AWS cluster.
    """

  @property
  def registered_nouns(self):
    if self.nouns is None:
      self.register_nouns()
    return self.nouns.keys()

  def _setup(self, args):
    GlobalCommandHookRegistry.setup(GLOBAL_HOOK_SKIP_RULES_URL)
    # Accessing registered_nouns has the side-effect of registering them, but since
    # nouns is unused, we must disable checkstyle.
    nouns = self.registered_nouns  # noqa
    for plugin in self.plugins:
      args = plugin.before_dispatch(args)
    return args

  def _parse_args(self, args):
    self.setup_options_parser()
    options = self.parser.parse_args(args)
    if options.noun not in self.nouns:
      raise ValueError("Unknown command: %s" % options.noun)
    noun = self.nouns[options.noun]
    context = noun.create_context()
    context.set_options(options)
    context.set_args(args)
    return (noun, context)

  def _run_pre_hooks_and_plugins(self, context, args):
    try:
      context.selected_hooks = GlobalCommandHookRegistry.get_required_hooks(context,
          context.options.skip_hooks, context.options.noun, context.options.verb)
    except Context.CommandError as c:
      return c.code
    try:
      for plugin in self.plugins:
        plugin.before_execution(context)
    except ConfigurationPlugin.Error as e:
      print("Error in configuration plugin before execution: %s" % e.msg, file=sys.stderr)
      return e.code
    plugin_result = GlobalCommandHookRegistry.run_pre_hooks(context, context.options.noun,
        context.options.verb)
    if plugin_result != EXIT_OK:
      return plugin_result
    else:
      return EXIT_OK

  def _run_post_plugins(self, context, result):
    for plugin in self.plugins:
      try:
        plugin.after_execution(context, result)
      except ConfigurationPlugin.Error as e:
        print_aurora_log(logging.INFO, "Error executing post-execution plugin: %s", e.msg)

  def _execute(self, args):
    """Execute a command.
    :param args: the command-line arguments for the command. This only includes arguments
        that should be parsed by the application; it does not include sys.argv[0].
    """
    try:
      args = self._setup(args)
    except ConfigurationPlugin.Error as e:
      print("Error in configuration plugin before dispatch: %s" % e.msg, file=sys.stderr)
      return e.code
    if args[0] == "help":
      return self.help_cmd(args[1:])
    noun, context = self._parse_args(args)
    print_aurora_log(TRANSCRIPT, "Command=(%s)", args)
    pre_result = self._run_pre_hooks_and_plugins(context, args)
    if pre_result is not EXIT_OK:
      return pre_result
    try:
      result = noun.execute(context)
      if result == EXIT_OK:
        print_aurora_log(TRANSCRIPT, "Command terminated successfully")
        GlobalCommandHookRegistry.run_post_hooks(context, context.options.noun,
            context.options.verb, result)
      else:
        print_aurora_log(logging.INFO, "Command terminated with error code %s", result)
      self._run_post_plugins(context, result)
      return result
    except Context.CommandError as c:
      print_aurora_log(logging.INFO, "Error executing command: %s", c.msg)
      self.print_err("Error executing command: %s" % c.msg)
      return c.code
    except Exception as e:
      print_aurora_log(logging.ERROR, "Internal error executing command: %s", e)
      return EXIT_API_ERROR

  def execute(self, args):
    try:
      return self._execute(args)
    except KeyboardInterrupt:
      print_aurora_log(logging.ERROR, "Command interrupted by user")
      return EXIT_INTERRUPTED
    except Exception as e:
      print_aurora_log(logging.ERROR, "Unknown error: %s" % e)
      if Context.reveal_errors():
        raise
      return EXIT_UNKNOWN_ERROR


class Noun(AuroraCommand):
  """A type of object manipulated by a command line application"""
  class InvalidVerbException(Exception): pass

  def __init__(self):
    super(Noun, self).__init__()
    self.verbs = {}
    self.commandline = None

  def set_commandline(self, commandline):
    self.commandline = commandline

  def register_verb(self, verb):
    """Add an operation supported for this noun."""
    if not isinstance(verb, Verb):
      raise TypeError("register_verb requires a Verb argument")
    self.verbs[verb.name] = verb
    verb._register(self)

  def internal_setup_options_parser(self, argparser):
    """Internal driver for the options processing framework.
    This gets the options from all of the verb for this noun, and assembles them
    into a python argparse subparser for this noun.
    """
    self.setup_options_parser(argparser)
    subparser = argparser.add_subparsers(dest="verb")
    for (name, verb) in self.verbs.items():
      vparser = subparser.add_parser(name, help=verb.help)
      for opt in verb.get_options():
        opt.add_to_parser(vparser)
      for plugin in self.commandline.plugins:
        for opt in plugin.get_options():
          opt.add_to_parser(vparser)
      for opt in GlobalCommandHookRegistry.get_options():
        opt.add_to_parser(vparser)

  @property
  def usage(self):
    return ["%s %s" % (self.name, self.verbs[verb].usage) for verb in self.verbs]

  @classmethod
  def create_context(cls):
    """Commands access state through a context object. The noun specifies what kind
    of context should be created for this noun's required state.
    """
    pass

  @property
  def composed_help(self):
    result = ['Usage for noun "%s":' % self.name]
    result += ["    %s %s" % (self.name, self.verbs[verb].usage) for verb in self.verbs]
    result += [self.help]
    return "\n\n".join(result)

  def execute(self, context):
    if context.options.verb not in self.verbs:
      raise self.InvalidVerbException("Noun %s does not have a verb %s" %
          (self.name, context.options.verb))
    return self.verbs[context.options.verb].execute(context)


class Verb(AuroraCommand):
  """An operation for a noun. Most application logic will live in verbs."""

  def _register(self, noun):
    """Create a link from a verb to its noun."""
    self.noun = noun

  @property
  def usage(self):
    """Get a brief usage-description for the command.
    A default usage string is automatically generated, but for commands with many options,
    users may want to specify usage themselves.
    """
    result = [self.name]
    result += [opt.render_usage() for opt in self.get_options()]
    return " ".join(result)

  @abstractmethod
  def get_options(self):
    pass

  @property
  def composed_help(self):
    """Generate the composed help message shown when the user requests help about this verb"""
    result = ['Usage for verb "%s %s":' % (self.noun.name, self.name)]
    result += ["  " + self.usage]
    result += ["Options:"]
    for opt in self.get_options():
      result += ["  " + s for s in opt.render_help()]
    for plugin in self.noun.commandline.plugins:
      for opt in plugin.get_options():
        result += ["  " + s for s in opt.render_help()]
    result += ["", self.help]
    return "\n".join(result)

  def execute(self, context):
    pass
