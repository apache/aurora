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

import java.util.List;
import javax.inject.Inject;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

import org.apache.aurora.common.inject.TimedInterceptor;
import org.apache.aurora.scheduler.stats.CachedCounters;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.scheduler.Mesos;
import org.apache.mesos.v1.scheduler.Protos.Call;
import org.apache.mesos.v1.scheduler.Protos.Event;
import org.apache.mesos.v1.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of Scheduler callback interfaces for the V1 Driver.
 */
public class VersionedMesosSchedulerImpl implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(VersionedMesosSchedulerImpl.class);

  private final CachedCounters counters;
  private final MesosCallbackHandler handler;
  private final Storage storage;
  private final FrameworkInfoFactory infoFactory;

  private volatile boolean isRegistered = false;

  private static final String EVENT_COUNTER_STAT_PREFIX = "mesos_scheduler_event_";
  // A cache to hold the metric names to prevent us from creating strings for every event
  private final LoadingCache<Event.Type, String> eventMetricNameCache = CacheBuilder.newBuilder()
      .maximumSize(Event.Type.values().length)
      .initialCapacity(Event.Type.values().length)
      .build(new CacheLoader<Event.Type, String>() {
        @Override
        public String load(Event.Type key) throws Exception {
          return EVENT_COUNTER_STAT_PREFIX + key.name();
        }
      });

  @Inject
  VersionedMesosSchedulerImpl(
      MesosCallbackHandler handler,
      CachedCounters counters,
      Storage storage,
      FrameworkInfoFactory factory) {
    this.handler = requireNonNull(handler);
    this.counters = requireNonNull(counters);
    this.storage = requireNonNull(storage);
    this.infoFactory = requireNonNull(factory);
    initializeEventMetrics();
  }

  @Override
  public void connected(Mesos mesos) {
    LOG.info("Connected to Mesos master.");

    Optional<String> frameworkId = storage.read(
        storeProvider -> storeProvider.getSchedulerStore().fetchFrameworkId());

    Protos.FrameworkInfo.Builder frameworkBuilder = infoFactory.getFrameworkInfo().toBuilder();

    Call.Builder call = Call.newBuilder().setType(Call.Type.SUBSCRIBE);

    if (frameworkId.isPresent()) {
      LOG.info("Found persisted framework ID: " + frameworkId);
      Protos.FrameworkID id = Protos.FrameworkID.newBuilder().setValue(frameworkId.get()).build();
      frameworkBuilder.setId(id);
      call.setFrameworkId(id);
    } else {
      frameworkBuilder.clearId();
      call.clearFrameworkId();
      LOG.warn("Did not find a persisted framework ID, connecting as a new framework.");
    }

    LOG.info("Sending subscribe call");
    mesos.send(call.setSubscribe(Call.Subscribe.newBuilder()
        .setFrameworkInfo(frameworkBuilder.build())
        .build())
        .build());
  }

  @Override
  public void disconnected(Mesos mesos) {
    handler.handleDisconnection();
  }

  private void initializeEventMetrics() {
    // For variable named metrics that are keyed on mesos enums, this ensures that we set
    // all possible metrics to 0.
    for (Event.Type type : Event.Type.values()) {
      this.counters.get(eventMetricNameCache.getUnchecked(type));
    }
  }

  private void countEventMetrics(Event event) {
    this.counters.get(eventMetricNameCache.getUnchecked(event.getType())).incrementAndGet();
  }

  @TimedInterceptor.Timed("scheduler_received")
  @Override
  public void received(Mesos mesos, Event event) {
    countEventMetrics(event);
    switch(event.getType()) {
      case SUBSCRIBED:
        Event.Subscribed subscribed = event.getSubscribed();
        if (isRegistered) {
          handler.handleReregistration(subscribed.getMasterInfo());
        } else {
          handler.handleRegistration(subscribed.getFrameworkId(), subscribed.getMasterInfo());
          isRegistered = true;
        }
        break;

      case OFFERS:
        checkState(isRegistered, "Must be registered before receiving offers.");
        handler.handleOffers(event.getOffers().getOffersList());
        break;

      case RESCIND:
        handler.handleRescind(event.getRescind().getOfferId());
        break;

      case INVERSE_OFFERS:
        List<Protos.InverseOffer> offers = event.getInverseOffers().getInverseOffersList();
        String ids = Joiner.on(",").join(
            Lists.transform(offers, input -> input.getId().getValue()));
        LOG.warn("Ignoring inverse offers: {}", ids);
        break;

      case RESCIND_INVERSE_OFFER:
        Protos.OfferID id = event.getRescindInverseOffer().getInverseOfferId();
        LOG.warn("Ignoring rescinded inverse offer: {}", id);
        break;

      case UPDATE:
        Protos.TaskStatus status = event.getUpdate().getStatus();
        handler.handleUpdate(status);
        break;

      case MESSAGE:
        Event.Message m = event.getMessage();
        handler.handleMessage(m.getExecutorId(), m.getAgentId());
        break;

      case ERROR:
        handler.handleError(event.getError().getMessage());
        break;

      case FAILURE:
        Event.Failure failure = event.getFailure();
        if (failure.hasExecutorId()) {
          handler.handleLostExecutor(
              failure.getExecutorId(),
              failure.getAgentId(),
              failure.getStatus());
        } else {
          handler.handleLostAgent(failure.getAgentId());
        }
        break;

      // TODO(zmanji): handle HEARTBEAT in a graceful manner
      // For now it is ok to silently ignore heart beats because the driver wil
      // detect disconnections for us.
      case HEARTBEAT:
        break;

      default:
        LOG.warn("Unknown event from Mesos \n{}", event);
        break;
    }
  }
}
