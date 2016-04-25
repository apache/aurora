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
package org.apache.aurora.scheduler.resources;

import org.apache.aurora.gen.Resource;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ResourceTypeTest {
  @Test
  public void testFindValueById() {
    assertEquals(
        ResourceType.CPUS,
        ResourceType.fromIdValue(Resource.numCpus(1.0).getSetField().getThriftFieldId()));
  }

  @Test
  public void testFindByResource() {
    assertEquals(
        ResourceType.CPUS,
        ResourceType.fromResource(IResource.build(Resource.numCpus(1.0))));
  }
}
