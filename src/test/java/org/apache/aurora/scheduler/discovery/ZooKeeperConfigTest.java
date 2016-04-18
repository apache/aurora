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

import java.net.InetSocketAddress;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.zookeeper.Credentials;
import org.apache.aurora.common.zookeeper.ZooKeeperUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class ZooKeeperConfigTest {

  private static final ImmutableList<InetSocketAddress> SERVERS =
      ImmutableList.of(InetSocketAddress.createUnresolved("localhost", 42));

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyServers() {
    new ZooKeeperConfig(
        false,
        ImmutableList.of(),
        Optional.absent(),
        false,
        Amount.of(1, Time.DAYS),
        Optional.absent());
  }

  @Test
  public void testWithCredentials() {
    ZooKeeperConfig config =
        new ZooKeeperConfig(
            false,
            SERVERS,
            Optional.absent(),
            false,
            Amount.of(1, Time.HOURS),
            Optional.absent()); // credentials
    assertFalse(config.getCredentials().isPresent());

    Credentials joeCreds = Credentials.digestCredentials("Joe", "Schmoe");
    ZooKeeperConfig joeConfig = config.withCredentials(joeCreds);

    // Should not mutate the original.
    assertNotSame(config, joeConfig);
    assertFalse(config.getCredentials().isPresent());

    assertTrue(joeConfig.getCredentials().isPresent());
    assertEquals(joeCreds, joeConfig.getCredentials().get());
  }

  @Test
  public void testCreateFactory() {
    ZooKeeperConfig config = ZooKeeperConfig.create(true, SERVERS);

    assertTrue(config.isUseCurator());
    assertEquals(SERVERS, ImmutableList.copyOf(config.getServers()));
    assertFalse(config.getChrootPath().isPresent());
    assertFalse(config.isInProcess());
    assertEquals(ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT, config.getSessionTimeout());
    assertFalse(config.getCredentials().isPresent());
  }
}
