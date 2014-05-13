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

from pystachio import Ref


class ParseError(Exception):
  pass

def add_port_to(option_name):
  def add_port_callback(option, opt, value, parser):
    if not getattr(parser.values, option_name, None):
      setattr(parser.values, option_name, {})
    try:
      name, port = value.split(':')
    except (ValueError, TypeError):
      raise ParseError('Invalid value for %s: %s should be of form NAME:PORT' % (
        opt, value))
    try:
      port = int(port)
    except ValueError:
      raise ParseError('Port does not appear to be an integer: %s' % port)
    getattr(parser.values, option_name)[name] = port
  return add_port_callback

def add_binding_to(option_name):
  def add_binding_callback(option, opt, value, parser):
    if not getattr(parser.values, option_name, None):
      setattr(parser.values, option_name, [])
    if len(value.split('=')) != 2:
      raise ParseError('Binding must be of the form NAME=VALUE')
    name, value = value.split('=')
    try:
      ref = Ref.from_address(name)
    except Ref.InvalidRefError as e:
      raise ParseError('Could not parse ref %s: %s' % (name, e))
    getattr(parser.values, option_name).append({ref: value})
  return add_binding_callback
