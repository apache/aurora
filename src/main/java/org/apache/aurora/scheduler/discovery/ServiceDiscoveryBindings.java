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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.InetSocketAddress;
import java.util.List;

import javax.inject.Qualifier;

import com.google.inject.Key;
import com.google.inject.TypeLiteral;

import org.apache.zookeeper.data.ACL;

/**
 * Useful constants for Guice modules that provide or consume service discovery configuration
 * bindings.
 */
public final class ServiceDiscoveryBindings {

  /**
   * Indicates a binding for ZooKeeper configuration data.
   */
  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
  public @interface ZooKeeper { }

  /**
   * A binding key for the ZooKeeper cluster endpoints.
   */
  public static final Key<Iterable<InetSocketAddress>> ZOO_KEEPER_CLUSTER_KEY =
      Key.get(new TypeLiteral<Iterable<InetSocketAddress>>() { }, ZooKeeper.class);

  /**
   * A binding key for the ZooKeeper ACL to use when creating nodes.
   */
  static final Key<List<ACL>> ZOO_KEEPER_ACL_KEY =
      Key.get(new TypeLiteral<List<ACL>>() { }, ZooKeeper.class);

  private ServiceDiscoveryBindings() {
    // Utility.
  }
}
