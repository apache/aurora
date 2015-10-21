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
package org.apache.aurora.scheduler.thrift;

import java.util.Objects;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Provider;

import com.google.common.annotations.VisibleForTesting;

import org.apache.shiro.subject.Subject;

/**
 * Generates audit messages with usernames from Shiro if available and falls back to
 * caller-specified ones if not.
 */
@VisibleForTesting
class AuditMessages {
  @VisibleForTesting
  static final String DEFAULT_USER = "UNSECURE";

  private final Provider<Optional<Subject>> subjectProvider;

  @Inject
  AuditMessages(Provider<Optional<Subject>> subjectProvider) {
    this.subjectProvider = Objects.requireNonNull(subjectProvider);
  }

  String getRemoteUserName() {
    return subjectProvider.get()
        .map(Subject::getPrincipal)
        .map(Object::toString)
        .orElse(DEFAULT_USER);
  }

  com.google.common.base.Optional<String> transitionedBy() {
    return com.google.common.base.Optional.of("Transition forced by " + getRemoteUserName());
  }

  com.google.common.base.Optional<String> killedByRemoteUser() {
    return com.google.common.base.Optional.of("Killed by " + getRemoteUserName());
  }

  com.google.common.base.Optional<String> restartedByRemoteUser() {
    return com.google.common.base.Optional.of("Restarted by " + getRemoteUserName());
  }
}
