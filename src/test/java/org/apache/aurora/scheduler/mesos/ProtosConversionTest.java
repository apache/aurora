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

import java.nio.ByteBuffer;
import java.util.UUID;

import com.google.protobuf.ByteString;

import org.apache.mesos.Protos;
import org.junit.Test;

import static org.apache.aurora.scheduler.mesos.ProtosConversion.convert;
import static org.junit.Assert.assertEquals;

public class ProtosConversionTest {

  @Test
  public void testOfferIDRoundTrip() {
    Protos.OfferID offerID = Protos.OfferID.newBuilder().setValue("offer-id").build();

    assertEquals(offerID, convert(convert(offerID)));
  }

  @Test
  public void testOfferRoundTrip() {
    Protos.Offer offer = Protos.Offer.newBuilder()
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id").build())
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave-id").build())
        .setHostname("hostname")
        .setId(Protos.OfferID.newBuilder().setValue("offer-id").build())
        .build();

    assertEquals(offer, convert(convert(offer)));
  }

  @Test
  public void testTaskStatusRoundTrip() {
    assertEquals(getStatus(), convert(convert(getStatus())));
  }

  @Test
  public void testTaskStatusConvertsAgentId() {
    Protos.TaskStatus status = getStatus();
    assertEquals(status.getSlaveId().getValue(), convert(status).getAgentId().getValue());
  }

  private Protos.TaskStatus getStatus() {
    UUID uuid = UUID.randomUUID();
    ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(uuid.getMostSignificantBits());
    buf.putLong(uuid.getLeastSignificantBits());

    return Protos.TaskStatus.newBuilder()
        .setUuid(ByteString.copyFrom(buf))
        .setTaskId(
            Protos.TaskID.newBuilder()
                .setValue("www-data-prod-hello-0-e3a2e294-a511-441c-a6bf-b8b4579029e7")
                .build())
        .setState(Protos.TaskState.TASK_RUNNING)
        .setMessage("Reconciliation: Latest task state")
        .setSlaveId(Protos.SlaveID.newBuilder()
            .setValue("10718383-5b7b-450f-8581-865134b3920c-S0")
            .build())
        .setTimestamp(1.4864285213798552E9)
        .build();
  }
}
