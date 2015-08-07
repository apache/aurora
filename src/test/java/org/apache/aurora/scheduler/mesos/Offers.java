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

import com.twitter.common.collections.Pair;

import org.apache.aurora.scheduler.Resources;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;

public final class Offers {
  private Offers() {
    // Utility class.
  }

  public static Offer createOffer(
      double cpu,
      double ramMb,
      double diskMb,
      Pair<Integer, Integer> portRange) {

    Ranges portRanges = Ranges.newBuilder()
        .addRange(Range
            .newBuilder().setBegin(portRange.getFirst()).setEnd(portRange.getSecond()).build())
        .build();

    return Offer.newBuilder()
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.CPUS)
            .setScalar(Scalar.newBuilder().setValue(cpu)))
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.RAM_MB)
            .setScalar(Scalar.newBuilder().setValue(ramMb)))
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.DISK_MB)
            .setScalar(Scalar.newBuilder().setValue(diskMb)))
        .addResources(Resource.newBuilder().setType(Type.RANGES).setName(Resources.PORTS)
            .setRanges(portRanges))
        .addAttributes(Protos.Attribute.newBuilder().setType(Type.TEXT)
            .setName("host")
            .setText(Protos.Value.Text.newBuilder().setValue("slavehost")))
        .setSlaveId(SlaveID.newBuilder().setValue("SlaveId").build())
        .setHostname("slavehost")
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id").build())
        .setId(OfferID.newBuilder().setValue("OfferId").build())
        .build();
  }
}
