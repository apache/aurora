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

from __future__ import print_function

from abc import abstractmethod
import logging
import os
import sys

from twitter.common.lang import Compatibility


class GlobalCommandHookRegistry(object):
  """Registry for command hooks.
  See docs/design/command-hooks.md for details on what hooks are, and how
  they work.
  """

  COMMAND_HOOKS = []
  HOOKS_FILE_NAME = 'AuroraHooks'

  @classmethod
  def load_hooks_file(cls, path):
    """Load a file containing hooks. If there are any errors compiling or executing the file,
    the errors will be logged, the hooks from the file will be skipped, but the execution of
    the command will continue.
    """
    #  To load a hooks file, we compile and exec the file.
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
        logging.warn('Warning: error loading hooks file %s: %s' % (path, e))
        print('Warning: error loading hooks file %s: %s' % (path, e), file=sys.stderr)
        return {}
      for hook in hooks_environment.get('hooks', []):
        cls.register_command_hook(hook)
      return hooks_environment

  @classmethod
  def find_project_hooks_file(self, dir):
    """Search for a file named "AuroraHooks" in  current directory or
    one of its parents, up to the closest repository root. Only one
    file will be loaded, so creating an AuroraHooks file in a subdirecory will
    override and replace the one in the parent directory
    """
    def is_repos_root(dir):
      # a directory is a git root if it contains a directory named ".git".
      # it's an HG root if it contains a directory named ".hg"
      return any(os.path.isdir(os.path.join(dir, rootname)) for rootname in ['.git', '.hg'])

    filepath =  os.path.join(dir, self.HOOKS_FILE_NAME)
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
        return find_project_hooks_file(parent)
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
    cls.COMMAND_HOOKS = []

  @classmethod
  def get_command_hooks_for(cls, noun, verb):
    """Looks up the set of hooks tht should apply for a (noun, verb) pair.
    """
    return [hook for hook in cls.COMMAND_HOOKS if noun in hook.get_nouns() and
        verb in hook.get_verbs(noun)]

  @classmethod
  def register_command_hook(cls, hook):
    cls.COMMAND_HOOKS.append(hook)


class CommandHook(object):
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

