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
package org.apache.aurora.common.zookeeper;

import org.apache.aurora.common.zookeeper.testing.BaseZooKeeperTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author John Sirois
 */
public class ZooKeeperUtilsTest extends BaseZooKeeperTest {

  @Test
  public void testNormalizingPath() throws Exception {
    assertEquals("/", ZooKeeperUtils.normalizePath("/"));
    assertEquals("/foo", ZooKeeperUtils.normalizePath("/foo/"));
    assertEquals("/foo/bar", ZooKeeperUtils.normalizePath("/foo//bar"));
    assertEquals("/foo/bar", ZooKeeperUtils.normalizePath("//foo/bar"));
    assertEquals("/foo/bar", ZooKeeperUtils.normalizePath("/foo/bar/"));
    assertEquals("/foo/bar", ZooKeeperUtils.normalizePath("/foo/bar//"));
    assertEquals("/foo/bar", ZooKeeperUtils.normalizePath("/foo/bar"));
  }

  @Test
  public void testLenientPaths() {
    assertEquals("/", ZooKeeperUtils.normalizePath("///"));
    assertEquals("/a/group", ZooKeeperUtils.normalizePath("/a/group"));
    assertEquals("/a/group", ZooKeeperUtils.normalizePath("/a/group/"));
    assertEquals("/a/group", ZooKeeperUtils.normalizePath("/a//group"));
    assertEquals("/a/group", ZooKeeperUtils.normalizePath("/a//group//"));

    try {
      ZooKeeperUtils.normalizePath("a/group");
      fail("Relative paths should not be allowed.");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      ZooKeeperUtils.normalizePath("/a/./group");
      fail("Relative paths should not be allowed.");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      ZooKeeperUtils.normalizePath("/a/../group");
      fail("Relative paths should not be allowed.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
