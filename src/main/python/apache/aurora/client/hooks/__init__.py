#
# Copyright 2013 Apache Software Foundation
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

"""
A hooks implementation for the Aurora client.

The Hook protocol is the following:
  Any object may be passed in as a hook.

  If the object has pre_<api method name> defined that is callable, it will be called with:
    method(*args, **kw)

  where *args and **kw are the arguments and keyword arguments passed into
  the original APi call.  This is done prior to the invocation of the API
  call.  If this method returns Falsy, the API call will be aborted.

  If the object has an err_<api method name> defined that is callable, it will be called with:
    method(exc, *args, **kw)

  If the object has a post_<api method name> defined that is callable, it will be called with:
    method(result, *args, **kw)

  These methods are called after the respective API call has been made.  The
  return codes of err and post methods are ignored.

If the object does not have any of these attributes, it will instead delegate to the
'generic_hook' method, if available.  The method signature for generic_hook is:

  generic_hook(hook_config, event, method_name, result_or_err, args, kw)

Where hook_config is a namedtuple of 'config' and 'job_key', event is one of
'pre', 'err', 'post', method_name is the API method name, and args, kw are
the arguments / keyword arguments.  result_or_err is a tri_state:
  - None for pre hooks
  - result for post hooks
  - exc for err hooks

Examples:

  class Logger(object):
    '''Just logs every at all point for all API calls'''
    def generic_hook(self, hook_config, event, method_name, result_or_err, *args, **kw)
       log.info('%s: %s_%s of %s' % (self.__class__.__name__, event, method_name, job_key))

  class KillConfirmer(object):
    def confirm(self, msg):
      return True if raw_input(msg).lower() == 'yes' else False

    def pre_kill(self, job_key, shards=None):
      shards = ('shards %s' % shards) if shards is not None else 'all shards'
      return self.confirm('Are you sure you want to kill %s? (yes/no): ' % (job_key, shards))
"""
