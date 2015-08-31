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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.common.io.Codec;
import org.apache.aurora.common.thrift.Endpoint;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.thrift.Status;
import org.apache.zookeeper.data.ACL;

/**
 * Common ServerSet related functions
 */
public class ServerSets {

  private ServerSets() {
    // Utility class.
  }

  /**
   * A function that invokes {@link #toEndpoint(InetSocketAddress)}.
   */
  public static final Function<InetSocketAddress, Endpoint> TO_ENDPOINT =
      ServerSets::toEndpoint;

  /**
   * Creates a server set that registers at a single path applying the given ACL to all nodes
   * created in the path.
   *
   * @param zkClient ZooKeeper client to register with.
   * @param acl The ACL to apply to the {@code zkPath} nodes the ServerSet creates.
   * @param zkPath Path to register at.  @see #create(ZooKeeperClient, java.util.Set)
   * @return A server set that registers at {@code zkPath}.
   */
  public static ServerSet create(ZooKeeperClient zkClient, Iterable<ACL> acl, String zkPath) {
    Preconditions.checkNotNull(zkClient);
    MorePreconditions.checkNotBlank(acl);
    MorePreconditions.checkNotBlank(zkPath);

    return new ServerSetImpl(zkClient, acl, zkPath);
  }

  /**
   * Returns a serialized Thrift service instance object, with given endpoints and codec.
   *
   * @param serviceInstance the Thrift service instance object to be serialized
   * @param codec the codec to use to serialize a Thrift service instance object
   * @return byte array that contains a serialized Thrift service instance
   */
  public static byte[] serializeServiceInstance(
      ServiceInstance serviceInstance, Codec<ServiceInstance> codec) throws IOException {

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    codec.serialize(serviceInstance, output);
    return output.toByteArray();
  }

  /**
   * Serializes a service instance based on endpoints.
   * @see #serializeServiceInstance(ServiceInstance, Codec)
   *
   * @param address the target address of the service instance
   * @param additionalEndpoints additional endpoints of the service instance
   * @param status service status
   */
  public static byte[] serializeServiceInstance(
      InetSocketAddress address,
      Map<String, Endpoint> additionalEndpoints,
      Status status,
      Codec<ServiceInstance> codec) throws IOException {

    ServiceInstance serviceInstance =
        new ServiceInstance(toEndpoint(address), additionalEndpoints, status);
    return serializeServiceInstance(serviceInstance, codec);
  }

  /**
   * Creates a service instance object deserialized from byte array.
   *
   * @param data the byte array contains a serialized Thrift service instance
   * @param codec the codec to use to deserialize the byte array
   */
  public static ServiceInstance deserializeServiceInstance(
      byte[] data, Codec<ServiceInstance> codec) throws IOException {

    return codec.deserialize(new ByteArrayInputStream(data));
  }

  /**
   * Creates an endpoint for the given InetSocketAddress.
   *
   * @param address the target address to create the endpoint for
   */
  public static Endpoint toEndpoint(InetSocketAddress address) {
    return new Endpoint(address.getHostName(), address.getPort());
  }
}
