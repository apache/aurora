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
  private final Provider<Optional<Subject>> subjectProvider;

  @Inject
  AuditMessages(Provider<Optional<Subject>> subjectProvider) {
    this.subjectProvider = Objects.requireNonNull(subjectProvider);
  }

  private String getShiroUserNameOr(String defaultUser) {
    return subjectProvider.get()
        .map(Subject::getPrincipal)
        .map(Object::toString)
        .orElse(defaultUser);
  }

  com.google.common.base.Optional<String> transitionedBy(String user) {
    return com.google.common.base.Optional.of("Transition forced by " + getShiroUserNameOr(user));
  }

  com.google.common.base.Optional<String> killedBy(String user) {
    return com.google.common.base.Optional.of("Killed by " + getShiroUserNameOr(user));
  }

  com.google.common.base.Optional<String> restartedBy(String user) {
    return com.google.common.base.Optional.of("Restarted by " + getShiroUserNameOr(user));
  }
}
