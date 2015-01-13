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

from abc import abstractmethod, abstractproperty

from twitter.common.lang import AbstractClass

from apache.aurora.client.cli.options import CommandOption

ALL_HOOKS = "all"


class GlobalCommandHookRegistry(object):
  """Registry for command hooks."""

  COMMAND_HOOKS = []

  @classmethod
  def setup(cls):
    pass

  @classmethod
  def get_options(cls):
    """Returns the options that should be added to option parsers for the hooks registry."""
    return [CommandOption("--skip-hooks", default=None,
        metavar="hook,hook,...",
        help=("A comma-separated list of command hook names that should be skipped. If the hooks"
              " cannot be skipped, then the command will be aborted"))]

  @classmethod
  def reset(cls):
    """For testing purposes, reset the list of registered hooks"""
    cls.COMMAND_HOOKS = []

  @classmethod
  def get_command_hooks_for(cls, noun, verb):
    """Looks up the set of hooks tht should apply for a (noun, verb) pair.
    """
    return [hook for hook in cls.COMMAND_HOOKS if noun in hook.get_nouns() and
        verb in hook.get_verbs(noun)]

  @classmethod
  def get_required_hooks(cls, context, skip_opt, noun, verb):
    """Given a set of hooks that match a command, find the set of hooks that
    must be run. If the user asked to skip a hook that cannot be skipped,
    raise an exception.
    """
    selected_hooks = cls.get_command_hooks_for(noun, verb)
    # The real set of required hooks is the set of hooks that match the command
    # being executed, minus the set of hooks that the user both wants to skip,
    # and is allowed to skip.
    if skip_opt is None:
      return selected_hooks
    selected_hook_names = [hook.name for hook in selected_hooks]
    desired_skips = set(selected_hook_names if skip_opt == ALL_HOOKS else skip_opt.split(","))
    desired_skips = desired_skips & set(selected_hook_names)
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
          context.print_out("Command aborted by command hook %s with error code %s"
                            % (hook.name, result))
          return result
      return 0
    except CommandHook.Error as c:
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
      context.print_err("Error executing command hook %s: %s; aborting" % hook.name, c.msg)
      return c.code


class CommandHook(AbstractClass):
  """A hook which contains code that should be run before certain commands."""
  class Error(Exception):
    def __init__(self, code, msg):
      super(CommandHook.Error, self).__init__(msg)  # noqa:T800
      self.code = code
      self.msg = msg

  @abstractproperty
  def name(self):
    "The name of the hook."

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
