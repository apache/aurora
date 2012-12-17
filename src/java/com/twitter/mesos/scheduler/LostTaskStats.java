package com.twitter.mesos.scheduler;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

import com.twitter.common.stats.Stats;
import com.twitter.common.stats.StatsProvider;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.storage.AttributeStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

/**
 * Tracks statistics about lost tasks.
 */
class LostTaskStats implements EventSubscriber {

  private static final Logger LOG = Logger.getLogger(LostTaskStats.class.getName());

  private final Storage storage;
  private final LoadingCache<String, AtomicLong> countersByRack;

  @VisibleForTesting
  LostTaskStats(Storage storage, final StatsProvider statsProvider) {
    this.storage = Preconditions.checkNotNull(storage);
    countersByRack = CacheBuilder.newBuilder()
        .build(new CacheLoader<String, AtomicLong>() {
          @Override public AtomicLong load(String rack) {
            return statsProvider.makeCounter(statName(rack));
          }
        });
  }

  @Inject
  LostTaskStats(Storage storage) {
    this(storage, Stats.STATS_PROVIDER);
  }

  @VisibleForTesting
  static String statName(String rack) {
    return "tasks_lost_rack_" + rack;
  }

  private static final Predicate<Attribute> IS_RACK = new Predicate<Attribute>() {
    @Override public boolean apply(Attribute attr) {
      return "rack".equals(attr.getName());
    }
  };

  private static final Function<Attribute, String> ATTR_VALUE = new Function<Attribute, String>() {
    @Override public String apply(Attribute attr) {
      return Iterables.getOnlyElement(attr.getValues());
    }
  };

  @Subscribe
  public void stateChange(TaskStateChange stateChange) {
    if (stateChange.getNewState() == ScheduleStatus.LOST) {
      final String host = stateChange.getTask().getAssignedTask().getSlaveHost();
      Optional<String> rack = storage.doInTransaction(new Work.Quiet<Optional<String>>() {
        @Override public Optional<String> apply(StoreProvider storeProvider) {
          Optional<Attribute> rack = FluentIterable
              .from(AttributeStore.Util.attributesOrNone(storeProvider, host))
              .firstMatch(IS_RACK);
          return rack.transform(ATTR_VALUE);
        }
      });

      if (rack.isPresent()) {
        countersByRack.getUnchecked(rack.get()).incrementAndGet();
      } else {
        LOG.warning("Failed to find rack attribute associated with host " + host);
      }
    }
  }
}
