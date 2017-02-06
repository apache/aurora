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
package org.apache.aurora.scheduler.mesos;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

import org.apache.mesos.v1.Protos;

/**
 * Utility class to convert to and from v1 and unversioned Mesos protobufs.
 *
 * The conversion process is the same process Mesos takes internally.
 *
 * Note that this should be package private, but some local simulator code needs to convert too.
 */
public final class ProtosConversion {
  private ProtosConversion() {
    // Utility Class
  }

  @SuppressWarnings("unchecked")
  private static <T extends MessageLite> T convert(MessageLite m, T.Builder builder) {
    ByteString data = m.toByteString();
    builder.clear();

    try {
      builder.mergeFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    return (T) builder.build();
  }

  // Methods to convert from unversioned to V1.

  public static Protos.Offer convert(org.apache.mesos.Protos.Offer o) {
    return convert(o, Protos.Offer.newBuilder());
  }

  public static Protos.OfferID convert(org.apache.mesos.Protos.OfferID id) {
    return convert(id, Protos.OfferID.newBuilder());
  }

  public static Protos.TaskStatus convert(org.apache.mesos.Protos.TaskStatus s) {
    return convert(s, Protos.TaskStatus.newBuilder());
  }

  // Methods to convert from V1 to unversioned.

  public static org.apache.mesos.Protos.FrameworkID convert(Protos.FrameworkID id) {
    return convert(id, org.apache.mesos.Protos.FrameworkID.newBuilder());
  }

  public static org.apache.mesos.Protos.TaskID convert(Protos.TaskID id) {
    return convert(id, org.apache.mesos.Protos.TaskID.newBuilder());
  }

  public static org.apache.mesos.Protos.OfferID convert(Protos.OfferID id) {
    return convert(id, org.apache.mesos.Protos.OfferID.newBuilder());
  }

  public static org.apache.mesos.Protos.Offer.Operation convert(Protos.Offer.Operation op) {
    return convert(op, org.apache.mesos.Protos.Offer.Operation.newBuilder());
  }

  public static org.apache.mesos.Protos.Filters convert(Protos.Filters f) {
    return convert(f, org.apache.mesos.Protos.Filters.newBuilder());
  }

  public static org.apache.mesos.Protos.TaskStatus convert(Protos.TaskStatus f) {
    return convert(f, org.apache.mesos.Protos.TaskStatus.newBuilder());
  }

  public static org.apache.mesos.Protos.FrameworkInfo convert(Protos.FrameworkInfo f) {
    return convert(f, org.apache.mesos.Protos.FrameworkInfo.newBuilder());
  }

  public static org.apache.mesos.Protos.Credential convert(Protos.Credential f) {
    return convert(f, org.apache.mesos.Protos.Credential.newBuilder());
  }

  public static org.apache.mesos.Protos.Offer convert(Protos.Offer f) {
    return convert(f, org.apache.mesos.Protos.Offer.newBuilder());
  }
}
