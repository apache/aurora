/**
 * Copyright 2013 Apache Software Foundation
 *
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
package org.apache.aurora.scheduler.thrift.auth;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotEmpty;

import org.apache.aurora.auth.CapabilityValidator;
import org.apache.aurora.auth.CapabilityValidator.Capability;
import org.apache.aurora.auth.SessionValidator;

/**
 * Binding module for authentication of users with special capabilities for admin functions.
 */
public class ThriftAuthModule extends AbstractModule {

  private static final Map<Capability, String> DEFAULT_CAPABILITIES =
      ImmutableMap.of(Capability.ROOT, "mesos");

  @NotEmpty
  @CmdLine(name = "user_capabilities",
      help = "Concrete name mappings for administration capabilities.")
  private static final Arg<Map<Capability, String>> USER_CAPABILITIES =
      Arg.create(DEFAULT_CAPABILITIES);

  private Map<Capability, String> capabilities;

  public ThriftAuthModule() {
    this(USER_CAPABILITIES.get());
  }

  @VisibleForTesting
  public ThriftAuthModule(Map<Capability, String> capabilities) {
    this.capabilities = Preconditions.checkNotNull(capabilities);
  }

  @Override
  protected void configure() {
    Preconditions.checkArgument(
        capabilities.containsKey(Capability.ROOT),
        "A ROOT capability must be provided with --user_capabilities");
    bind(new TypeLiteral<Map<Capability, String>>() { }).toInstance(capabilities);

    requireBinding(SessionValidator.class);
    requireBinding(CapabilityValidator.class);
  }
}
