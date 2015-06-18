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
package org.apache.aurora.auth;

import java.util.Optional;

import javax.annotation.Nullable;
import javax.inject.Provider;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

import com.twitter.common.stats.StatsProvider;
import org.apache.shiro.subject.Subject;

/**
 * Uses context from Shiro for audit messages if available, otherwise defaults to a placeholder
 * indicating the audit record is unsecure.
 */
class UnsecureSessionContext implements SessionValidator.SessionContext {
  @VisibleForTesting
  static final String SHIRO_AUDIT_LOGGING_ENABLED = "shiro_audit_logging_enabled";

  @VisibleForTesting
  static final String UNSECURE = "UNSECURE";

  @Inject
  UnsecureSessionContext(StatsProvider statsProvider) {
    statsProvider.makeGauge(SHIRO_AUDIT_LOGGING_ENABLED, () -> subjectProvider == null ? 0 : 1);
  }

  @Nullable
  private Provider<Subject> subjectProvider;

  @Inject(optional = true)
  void setSubjectProvider(Provider<Subject> subjectProvider) {
    this.subjectProvider = subjectProvider;
  }

  @Override
  public String getIdentity() {
    return Optional.ofNullable(subjectProvider)
        .map(Provider::get)
        .map(Subject::getPrincipals)
        .map((principalCollection) -> principalCollection.oneByType(String.class))
        .orElse(UNSECURE);
  }
}
