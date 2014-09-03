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
package org.apache.aurora.scheduler.app.local.simulator;

import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.base.Command;

import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.HOST_CONSTRAINT;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.RACK_CONSTRAINT;
import static org.apache.mesos.Protos.Value.Type.RANGES;
import static org.apache.mesos.Protos.Value.Type.SCALAR;

/**
 * Module that sets up bindings to simulate fake cluster resources.
 */
public class ClusterSimulatorModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(FakeSlaves.class).in(Singleton.class);
    Multibinder<Offer> offers = Multibinder.newSetBinder(binder(), Offer.class);
    offers.addBinding()
        .toInstance(baseOffer("slave-1", "a", 16, 16 * 1024, 100 * 1024));
    offers.addBinding()
        .toInstance(baseOffer("slave-2", "a", 16, 16 * 1024, 100 * 1024));
    offers.addBinding()
        .toInstance(baseOffer("slave-3", "b", 16, 16 * 1024, 100 * 1024));
    offers.addBinding()
        .toInstance(baseOffer("slave-4", "b", 16, 16 * 1024, 100 * 1024));
    offers.addBinding()
        .toInstance(dedicated(baseOffer("slave-5", "c", 24, 128 * 1024, 1824 * 1024), "database"));
    offers.addBinding()
        .toInstance(dedicated(baseOffer("slave-6", "c", 24, 128 * 1024, 1824 * 1024), "database"));
    LifecycleModule.bindStartupAction(binder(), Register.class);
  }

  static class Register implements Command {
    private final EventBus eventBus;
    private final FakeSlaves slaves;

    @Inject
    Register(EventBus eventBus, FakeSlaves slaves) {
      this.eventBus = requireNonNull(eventBus);
      this.slaves = requireNonNull(slaves);
    }

    @Override
    public void execute() {
      eventBus .register(slaves);
    }
  }

  private static Offer baseOffer(
      String slaveId,
      String rack,
      double cpu,
      double ramMb,
      double diskMb) {

    Protos.Value.Ranges portRanges = Protos.Value.Ranges.newBuilder()
        .addRange(Protos.Value.Range.newBuilder().setBegin(40000).setEnd(41000)).build();

    String host = slaveId + "-hostname";
    return Offer.newBuilder()
        .addResources(Protos.Resource.newBuilder().setType(SCALAR).setName(Resources.CPUS)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpu)))
        .addResources(Protos.Resource.newBuilder().setType(SCALAR).setName(Resources.RAM_MB)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(ramMb)))
        .addResources(Protos.Resource.newBuilder().setType(SCALAR).setName(Resources.DISK_MB)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(diskMb)))
        .addResources(Protos.Resource.newBuilder().setType(RANGES).setName(Resources.PORTS)
            .setRanges(portRanges))
        .addAttributes(Protos.Attribute.newBuilder().setType(Protos.Value.Type.TEXT)
            .setName(HOST_CONSTRAINT)
            .setText(Protos.Value.Text.newBuilder().setValue(host)))
        .addAttributes(Protos.Attribute.newBuilder().setType(Protos.Value.Type.TEXT)
            .setName(RACK_CONSTRAINT)
            .setText(Protos.Value.Text.newBuilder().setValue(rack)))
        .setSlaveId(Protos.SlaveID.newBuilder().setValue(slaveId))
        .setHostname(host)
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("frameworkId").build())
        .setId(Protos.OfferID.newBuilder().setValue(UUID.randomUUID().toString()))
        .build();
  }

  private static Offer dedicated(Offer base, String dedicatedTo) {
    return Offer.newBuilder(base)
        .addAttributes(Protos.Attribute.newBuilder().setType(Protos.Value.Type.TEXT)
            .setName(DEDICATED_ATTRIBUTE)
            .setText(Protos.Value.Text.newBuilder().setValue(dedicatedTo)))
        .build();
  }
}
