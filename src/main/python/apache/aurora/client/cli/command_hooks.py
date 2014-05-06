#
# Copyright 2014 Apache Software Foundation
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

# The implementation of command hooks. See the design doc in docs/design/command-hooks.md for
# details on what hooks are and how they work.

from __future__ import print_function

from abc import abstractmethod
from fnmatch import fnmatch
import getpass
import logging
import os
import sys

from apache.aurora.client.cli.options import CommandOption

import requests
from twitter.common.lang import Compatibility

# Ugly workaround to avoid cyclic dependency.
EXIT_PERMISSION_VIOLATION = 8


ALL_HOOKS = "all"


class SkipHooksRule(object):
  """A rule that specifies when a user may skip a command hook."""

  @property
  def name(self):
    pass

  def allow_hook_skip(self, hook, user, noun, verb, args):
    """ Check if this rule allows a user to skip a specific hook.
    Params:
    - hook: the name of the hook that the user wants to skip.
    - user: the user requesting that hooks be skipped. (Note: user, not role!)
    - noun, verb: the noun and verb being executed.
    - args: the other command-line arguments.
    Returns: True if the user should be allowed to skip the requested hooks.
    """
    return False


class JsonSkipHooksRule(SkipHooksRule):
  """An implementation for skip rules that are loaded from a json file.
  See the design doc for details on the format of the rules."""

  def __init__(self, name, json_rule):
    self.rulename = name
    self.hooks = json_rule.get("hooks", ["*"])
    self.users = json_rule.get("users", ["*"])
    self.commands = json_rule.get("commands", {})
    self.arg_patterns = json_rule.get("arg-patterns", None)

  @property
  def name(self):
    return self.rulename

  def allow_hook_skip(self, hook, user, noun, verb, args):
    def _hooks_match(hook):
      return any(fnmatch(hook, hook_pattern) for hook_pattern in self.hooks)

    def _commands_match(noun, verb):
      return noun in self.commands and verb in self.commands[noun]

    def _users_match(user):
      return any(fnmatch(user, user_pattern) for user_pattern in self.users)

    def _args_match(args):
      if self.arg_patterns is None:
        return True
      else:
        return any(fnmatch(arg, arg_pattern) for arg_pattern in self.arg_patterns
            for arg in args)

    return (_hooks_match(hook) and _commands_match(noun, verb) and _users_match(user) and
        _args_match(args))


class GlobalCommandHookRegistry(object):
  """Registry for command hooks.
  See docs/design/command-hooks.md for details on what hooks are, and how
  they work.
  """

  COMMAND_HOOKS = []
  HOOKS_FILE_NAME = "AuroraHooks"
  SKIP_HOOK_RULES = {}

  @classmethod
  def setup(cls, rules_url):
    """Initializes the hook registry, loading the project hooks and the skip rules."""
    if rules_url is not None:
      cls.load_global_hook_skip_rules(rules_url)
    cls.load_project_hooks()

  @classmethod
  def get_options(cls):
    """Returns the options that should be added to option parsers for the hooks registry."""
    return [CommandOption("--skip-hooks", default=None,
        metavar="hook,hook,...",
        help=("A comma-separated list of command hook names that should be skipped. If the hooks"
              " cannot be skipped, then the command will be aborted"))]

  @classmethod
  def register_hook_skip_rule(cls, rule):
    """Registers a rule to allow users to skip certain hooks"""
    cls.SKIP_HOOK_RULES[rule.name] = rule

  @classmethod
  def register_json_hook_skip_rules(cls, json_rules):
    """Register a set of skip rules that are structured as JSON dictionaries."""
    for (name, rule) in json_rules.items():
      cls.register_hook_skip_rule(JsonSkipHooksRule(name, rule))

  @classmethod
  def load_global_hook_skip_rules(cls, url):
    """If the system uses a master skip rules file, loads the master skip rules JSON."""
    resp = requests.get(url)
    try:
      rules = resp.json()
      cls.register_json_hook_skip_rules(rules)
    except ValueError as e:
      logging.error("Client could not decode hook skip rules: %s" % e)


  @classmethod
  def load_hooks_file(cls, path):
    """Load a file containing hooks. If there are any errors compiling or executing the file,
    the errors will be logged, the hooks from the file will be skipped, but the execution of
    the command will continue.
    """
    with open(path, "r") as hooks_file:
      hooks_data = hooks_file.read()
      hooks_code = None
      try:
        hooks_code = compile(hooks_data, path, "exec")
      except (SyntaxError, TypeError) as e:
        logging.warn("Error compiling hooks file %s: %s" % (path, e))
        print("Error compiling hooks file %s: %s" % (path, e), file=sys.stderr)
        return {}
      hooks_environment = {}
      try:
        Compatibility.exec_function(hooks_code, hooks_environment)
      except Exception as e:
        # Unfortunately, exec could throw *anything* at all.
        logging.warn("Warning: error loading hooks file %s: %s" % (path, e))
        print("Warning: error loading hooks file %s: %s" % (path, e), file=sys.stderr)
        return {}
      for hook in hooks_environment.get("hooks", []):
        cls.register_command_hook(hook)
      return hooks_environment

  @classmethod
  def find_project_hooks_file(cls, dir):
    """Search for a file named "AuroraHooks" in  current directory or
    one of its parents, up to the closest repository root. Only one
    file will be loaded, so creating an AuroraHooks file in a subdirecory will
    override and replace the one in the parent directory
    """
    def is_repos_root(dir):
      # a directory is a git root if it contains a directory named ".git".
      # it's an HG root if it contains a directory named ".hg"
      return any(os.path.isdir(os.path.join(dir, rootname)) for rootname in [".git", ".hg"])

    filepath =  os.path.join(dir, cls.HOOKS_FILE_NAME)
    if os.path.exists(filepath):
      return filepath
    elif is_repos_root(dir):
      # This is a repository root, so stop looking.
      return None
    else:
      parent, _ = os.path.split(os.path.abspath(dir))
      # if we've reached the filesystem root, then parent==self.
      if parent == dir:
        return None
      else:
        return cls.find_project_hooks_file(parent)
      return None

  @classmethod
  def load_project_hooks(cls, dir="."):
    """Looks for a project hooks file. Project hooks will be in the current directory,
    or one of its parents. Stops looking at the root of an SCM repository.
    """
    hooks_file = cls.find_project_hooks_file(dir)
    if hooks_file is not None:
      return cls.load_hooks_file(hooks_file)
    else:
      return None

  @classmethod
  def reset(cls):
    """For testing purposes, reset the list of registered hooks"""
    cls.SKIP_HOOK_RULES = {}
    cls.COMMAND_HOOKS = []

  @classmethod
  def get_command_hooks_for(cls, noun, verb):
    """Looks up the set of hooks tht should apply for a (noun, verb) pair.
    """
    return [hook for hook in cls.COMMAND_HOOKS if noun in hook.get_nouns() and
        verb in hook.get_verbs(noun)]

  @classmethod
  def get_required_hooks(cls, context, skip_opt, noun, verb, user=None):
    """Given a set of hooks that match a command, find the set of hooks that
    must be run. If the user asked to skip a hook that cannot be skipped,
    raise an exception.
    """
    if user is None:
      user = getpass.getuser()
    selected_hooks = cls.get_command_hooks_for(noun, verb)
    # The real set of required hooks is the set of hooks that match the command
    # being executed, minus the set of hooks that the user both wants to skip,
    # and is allowed to skip.
    if skip_opt is None:
      return selected_hooks
    selected_hook_names = [hook.name for hook in selected_hooks]
    desired_skips = set(selected_hook_names if skip_opt == ALL_HOOKS else skip_opt.split(","))
    desired_skips = desired_skips & set(selected_hook_names)
    for desired_skip in desired_skips:
      if not any(rule.allow_hook_skip(desired_skip, user, noun, verb, context.args)
          for name, rule in cls.SKIP_HOOK_RULES.items()):
        context.print_log(logging.INFO, "Hook %s cannot be skipped by user %s" %
            (desired_skip, user))
        raise context.CommandError(EXIT_PERMISSION_VIOLATION,
            "Hook %s cannot be skipped by user %s" % (desired_skip, user))
    selected_hook_names = set(selected_hook_names) - desired_skips
    return [hook for hook in selected_hooks if hook.name in selected_hook_names]

  @classmethod
  def register_command_hook(cls, hook):
    # "all" is a reserved name used to indicate that the user wants to skip all hooks, not just
    # a specific set.
    if hook.name == ALL_HOOKS:
      raise ValueError("Invalid hook name 'all'")
    cls.COMMAND_HOOKS.append(hook)

  @classmethod
  def run_pre_hooks(cls, context, noun, verb):
    """Run all of the non-skipped hooks that apply to this command."""
    pre_hooks = context.selected_hooks
    try:
      for hook in pre_hooks:
        result = hook.pre_command(noun, verb, context, context.args)
        if result != 0:
          context.print_log(logging.INFO, "Command hook %s aborted operation with error code %s" %
              (hook.name, result))
          context.print_out("Command aborted by command hook %s" % hook.name)
          return result
      return 0
    except CommandHook.Error as c:
      context.print_log(logging.INFO, "Error executing command hook %s: %s" % (hook.name, c))
      context.print_err("Error executing command hook %s: %s; aborting" % hook.name, c.msg)
      return c.code

  @classmethod
  def run_post_hooks(cls, context, noun, verb, result):
    """Run all of the non-skipped post-command hooks that apply to this command"""
    selected_hooks = context.selected_hooks
    try:
      for hook in selected_hooks:
        hook.post_command(noun, verb, context, context.args, result)
        return 0
    except CommandHook.Error as c:
      context.print_log(logging.INFO, "Error executing post-command hook %s: %s" % (hook.name, c))
      context.print_err("Error executing command hook %s: %s; aborting" % hook.name, c.msg)
      return c.code

class CommandHook(object):
  """A hook which contains code that should be run before certain commands."""
  class Error(Exception):
    def __init__(self, code, msg):
      super(CommandHook.Error, self).__init__(msg)
      self.code = code
      self.msg = msg

  @property
  def name(self):
    return None

  @abstractmethod
  def get_nouns(self):
    """Return the nouns that have verbs that should invoke this hook."""

  @abstractmethod
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
    Returns: 0 if the command should be allowed to proceed; otherwise, the exit code
       to return at the command line.
    """

  @abstractmethod
  def post_command(self, noun, verb, context, commandline, result):
    """Execute a hook after invoking a verb.
    * noun: the noun being invoked.
    * verb: the verb being invoked.
    * context: the context object that will be used to invoke the verb.
    * commandline: the original argv collection used to invoke the client.
    * result: the result code returned by the verb.
    Returns: nothing
    """
