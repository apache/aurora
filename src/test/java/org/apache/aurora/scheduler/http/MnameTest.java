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
package org.apache.aurora.scheduler.http;

import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MnameTest {

  @Test
  public void testRedirectPort() {
    assertEquals(Optional.absent(), getRedirectPort(null));
    assertEquals(Optional.absent(), getRedirectPort(ImmutableMap.<String, Integer>of()));
    assertEquals(Optional.absent(), getRedirectPort(ImmutableMap.of("thrift", 5)));
    assertEquals(Optional.of(5), getRedirectPort(ImmutableMap.of("health", 5, "http", 6)));
    assertEquals(Optional.of(6), getRedirectPort(ImmutableMap.of("http", 6)));
    assertEquals(Optional.of(7), getRedirectPort(ImmutableMap.of("HTTP", 7)));
    assertEquals(Optional.of(8), getRedirectPort(ImmutableMap.of("web", 8)));
  }

  private Optional<Integer> getRedirectPort(Map<String, Integer> ports) {
    return Mname.getRedirectPort(IAssignedTask.build(new AssignedTask().setAssignedPorts(ports)));
  }
}
