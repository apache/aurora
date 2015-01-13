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

'''Command-line tooling infrastructure for aurora client.

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
import logging
import sys
import traceback
from abc import abstractmethod, abstractproperty

import pkg_resources
from twitter.common.lang import AbstractClass, Compatibility

from .command_hooks import GlobalCommandHookRegistry
from .options import CommandOption

# Constants for standard return codes.
EXIT_OK = 0
EXIT_INTERRUPTED = 130
EXIT_INVALID_CONFIGURATION = 3
EXIT_COMMAND_FAILURE = 4
EXIT_INVALID_COMMAND = 5
EXIT_INVALID_PARAMETER = 6
EXIT_NETWORK_ERROR = 7
EXIT_TIMEOUT = 9
EXIT_API_ERROR = 10
EXIT_UNKNOWN_ERROR = 20


__version__ = pkg_resources.resource_string(__name__, '.auroraversion')


class Context(object):
  class Error(Exception): pass
  class ArgumentException(Error): pass

  class CommandError(Error):
    def __init__(self, code, msg):
      super(Context.CommandError, self).__init__(msg)  # noqa:T800
      self.msg = msg
      self.code = code

  class CommandErrorLogged(CommandError):
    def __init__(self, code, msg):
      super(Context.CommandErrorLogged, self).__init__(code, msg)  # noqa:T800

  def __init__(self):
    self._options = None

  @classmethod
  def exit(cls, code, msg):
    raise cls.CommandError(code, msg)

  @property
  def options(self):
    return self._options

  def set_options(self, options):
    """Add the options object to a context.
    This is separated from the constructor to make patching tests easier.
    """
    self._options = options

  def set_args(self, args):
    """Add the raw argument list to a context."""
    self.args = args

  def print_out(self, msg, indent=0):
    """Prints output to standard out with indent.
    For debugging purposes, it's nice to be able to patch this and capture output.
    """
    if not isinstance(msg, Compatibility.string):
      raise TypeError('msg must be a string')
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


class ConfigurationPlugin(AbstractClass):
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
    """Return the set of options processed by this plugin
    This must return a list of CommandOption objects that represent the arguments for this plugin.
    """

  @abstractmethod
  def before_dispatch(self, raw_args):
    """Run some code before dispatching to the client.
    If a ConfigurationPlugin.Error exception is thrown, aborts the command execution.
    This must return a list of arguments to pass to the remaining plugins and program.
    """

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


class AuroraCommand(AbstractClass):
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

  @abstractproperty
  def help(self):
    """Returns the help message for this command"""

  @abstractproperty
  def name(self):
    """Returns the command name"""


class CommandLine(AbstractClass):
  """The top-level object implementing a command-line application."""

  @abstractproperty
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
    self.parser.add_argument('--version', action='version', version=__version__)
    subparser = self.parser.add_subparsers(dest="noun", title='commands')
    for name, noun in self.nouns.items():
      noun_parser = subparser.add_parser(name, help=noun.help)
      noun.internal_setup_options_parser(noun_parser)

  @abstractmethod
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
    GlobalCommandHookRegistry.setup()
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
        logging.info("Error executing post-execution plugin: %s", e.msg)

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
    noun, context = self._parse_args(args)
    logging.debug("Command=(%s)", args)
    pre_result = self._run_pre_hooks_and_plugins(context, args)
    if pre_result is not EXIT_OK:
      return pre_result
    try:
      result = noun.execute(context)
      assert result is not None, "Command return value is None!"
      if result == EXIT_OK:
        logging.debug("Command terminated successfully")
        GlobalCommandHookRegistry.run_post_hooks(context, context.options.noun,
            context.options.verb, result)
      else:
        logging.info("Command terminated with error code %s", result)
      self._run_post_plugins(context, result)
      return result
    except Context.CommandErrorLogged as c:
      return c.code
    except Context.CommandError as c:
      self.print_err("Error executing command: %s" % c.msg)
      return c.code
    except Exception:
      print("Fatal error running command:", file=sys.stderr)
      print(traceback.format_exc(), file=sys.stderr)
      return EXIT_UNKNOWN_ERROR

  def execute(self, args):
    try:
      return self._execute(args)
    except KeyboardInterrupt:
      logging.error("Command interrupted by user")
      return EXIT_INTERRUPTED
    except Exception as e:
      logging.error("Unknown error: %s" % e)
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
    subparser = argparser.add_subparsers(dest="verb", title='subcommands')
    for name, verb in self.verbs.items():
      vparser = subparser.add_parser(name, help=verb.help)
      for opt in verb.get_options():
        opt.add_to_parser(vparser)
      for plugin in self.commandline.plugins:
        for opt in plugin.get_options():
          opt.add_to_parser(vparser)
      for opt in GlobalCommandHookRegistry.get_options():
        opt.add_to_parser(vparser)

  @classmethod
  def create_context(cls):
    """Commands access state through a context object. The noun specifies what kind
    of context should be created for this noun's required state.
    """
    pass

  def execute(self, context):
    if context.options.verb not in self.verbs:
      raise self.InvalidVerbException("Command %s does not have subcommand %s" %
          (self.name, context.options.verb))
    return self.verbs[context.options.verb].execute(context)


class Verb(AuroraCommand):
  """An operation for a noun. Most application logic will live in verbs."""

  def _register(self, noun):
    """Create a link from a verb to its noun."""
    self.noun = noun

  @abstractmethod
  def get_options(self):
    pass

  @abstractmethod
  def execute(self, context):
    pass
