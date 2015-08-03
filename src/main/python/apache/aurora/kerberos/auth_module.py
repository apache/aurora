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
from requests_kerberos import DISABLED, HTTPKerberosAuth

from apache.aurora.common.auth_module import AuthModule


class KerberosAuthModule(AuthModule):
  @property
  def mechanism(self):
    return 'KERBEROS'

  def payload(self):
    """NOTE: until AURORA-1229 is addressed, using "Kerberized" client in production in a backwards
             compatible way will require a new custom module that will override this method to
             return the currently used payload (security blob used in SessionKey).
    """
    return ''

  def auth(self):
    # While SPNEGO supports mutual authentication of the response, it does not assert the validity
    # of the response payload, only the identity of the server. Thus the scheduler will not set
    # the WWW-Authenticate response header and the client will disable mutual authentication.
    # In order to achieve communication with the scheduler subject to confidentiality and integrity
    # constraints the client must connect to the scheduler API via HTTPS. Kerberos is thus only
    # used to authenticate the client to the server.
    return HTTPKerberosAuth(mutual_authentication=DISABLED)
