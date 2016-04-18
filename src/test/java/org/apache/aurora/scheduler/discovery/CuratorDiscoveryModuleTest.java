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
package org.apache.aurora.scheduler.discovery;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.application.ShutdownRegistry.ShutdownRegistryImpl;
import org.apache.aurora.common.zookeeper.ZooKeeperUtils;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.data.ACL;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CuratorDiscoveryModuleTest extends AbstractDiscoveryModuleTest {

  @Override
  void bindExtraRequirements(Binder binder) {
    ShutdownRegistryImpl shutdownRegistry = new ShutdownRegistryImpl();
    binder.bind(ShutdownRegistry.class).toInstance(shutdownRegistry);
    addTearDown(shutdownRegistry::execute);
  }

  @Override
  Module createModule(String discoveryPath, ZooKeeperConfig zooKeeperConfig) {
    return new CuratorServiceDiscoveryModule(discoveryPath, zooKeeperConfig);
  }

  @Override
  boolean isCurator() {
    return false;
  }

  @Test
  public void testSingleACLProvider() {
    ImmutableList<ACL> acl = ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL;
    ACLProvider provider = new CuratorServiceDiscoveryModule.SingleACLProvider(acl);

    assertEquals(acl, provider.getDefaultAcl());
    assertEquals(acl, provider.getAclForPath("/random/path/1"));
    assertEquals(acl, provider.getAclForPath("/random/path/2"));
  }

  @Test(expected = NullPointerException.class)
  public void testSingleACLProviderNull() {
    new CuratorServiceDiscoveryModule.SingleACLProvider(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSingleACLProviderEmpty() {
    new CuratorServiceDiscoveryModule.SingleACLProvider(ImmutableList.of());
  }
}
