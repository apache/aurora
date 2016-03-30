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

# checkstyle: noqa

from pystachio import Boolean, Default, List, Required, String, Struct


class Auth(Struct):
  scheme     = Required(String)
  credential = Required(String)


class Permissions(Struct):
  read    = Default(Boolean, False)
  write   = Default(Boolean, False)
  create  = Default(Boolean, False)
  delete  = Default(Boolean, False)
  admin   = Default(Boolean, False)


class Access(Struct):
  scheme      = Required(String)
  credential  = Required(String)
  permissions = Required(Permissions)


class ZkAuth(Struct):
  auth = Default(List(Auth), [])
  acl  = Default(List(Access), [])
