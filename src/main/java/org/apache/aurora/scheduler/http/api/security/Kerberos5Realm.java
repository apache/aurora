/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.http.api.security;

import javax.inject.Inject;
import javax.security.auth.kerberos.KerberosPrincipal;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;

import static java.util.Objects.requireNonNull;

/**
 * Authentication-only realm for Kerberos V5.
 */
class Kerberos5Realm implements Realm {
  private static final Splitter AT_SPLITTER = Splitter.on("@");

  private final GSSManager gssManager;
  private final GSSCredential serverCredential;

  @Inject
  Kerberos5Realm(GSSManager gssManager, GSSCredential serverCredential) {
    this.gssManager = requireNonNull(gssManager);
    this.serverCredential = requireNonNull(serverCredential);
  }

  @Override
  public String getName() {
    return getClass().getName();
  }

  @Override
  public boolean supports(AuthenticationToken token) {
    return token instanceof AuthorizeHeaderToken;
  }

  @Override
  public AuthenticationInfo getAuthenticationInfo(AuthenticationToken token)
      throws AuthenticationException {

    byte[] tokenFromInitiator = ((AuthorizeHeaderToken) token).getAuthorizeHeaderValue();
    GSSContext context;
    try {
      context = gssManager.createContext(serverCredential);
      context.acceptSecContext(tokenFromInitiator, 0, tokenFromInitiator.length);
    } catch (GSSException e) {
      throw new AuthenticationException(e);
    }

    // Technically the GSS-API requires us to continue sending data back and forth in a loop
    // until the context is established, but we can short-circuit here since we know we're using
    // Kerberos V5 directly or Kerberos V5-backed SPNEGO. This is important because it means we
    // don't need to keep state between requests.
    // From http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/single-signon.html
    // "In the case of the Kerberos V5 mechanism, there is no more than one round trip of
    // tokens during context establishment."
    if (context.isEstablished()) {
      try {
        KerberosPrincipal kerberosPrincipal =
            new KerberosPrincipal(context.getSrcName().toString());
        return new SimpleAuthenticationInfo(
            new SimplePrincipalCollection(
                ImmutableList.<Object>of(
                    // We assume there's a single Kerberos realm in use here. Most Authorizer
                    // implementations care about the "simple" username instead of the full
                    // principal.
                    AT_SPLITTER.splitToList(kerberosPrincipal.getName()).get(0),
                    kerberosPrincipal),
                getName()),
            null /* There are no credentials that can be cached. */);
      } catch (GSSException | IndexOutOfBoundsException e) {
        throw new AuthenticationException(e);
      }
    } else {
      throw new AuthenticationException("GSSContext was not established with a single message.");
    }
  }
}
