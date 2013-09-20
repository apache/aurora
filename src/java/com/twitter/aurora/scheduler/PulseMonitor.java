/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

/**
 * A pulse monitor to identify when a pulse has not been received for an item beyond a defined
 * threshold.
 * Also acts as a supplier of the pulsed type, which will provide access to all non-expired
 * entries.
 *
 * @param <T> The type of values to track.
 */
public interface PulseMonitor<T> extends Supplier<Set<T>> {

  /**
   * Receive a pulse for an entry, effectively marking it as alive.
   *
   * @param t Item to update.
   */
  void pulse(T t);

  /**
   * Checks if an entry is considered alive, based on the expiration time.  Note that if the
   * monitor is created and this method is called before {@link #pulse(Object)},
   * this method will always return {@code false}.
   *
   * @param t Item to check the pulse of.
   * @return {@code true} if a pulse has been received for the entry, and the time between now and
   *    the last pulse is less than the expiration period, {@code false} otherwise.
   */
  boolean isAlive(T t);

  /**
   * Pulse monitor implementation using a decaying map for time expiration.
   *
   * @param <T> The type of values to track.
   */
  public static class PulseMonitorImpl<T> implements PulseMonitor<T> {

    private final Map<T, T> pulses;

    /**
     * Creates a new pulse monitor that will consider an entry dead if the time since the last pulse
     * for the entry is greater than {@code expiration}.
     *
     * @param expiration Time after which an entry is considered dead.
     */
    public PulseMonitorImpl(Amount<Long, Time> expiration) {
      // TODO(William Farner): Consider using timestamps instead and allowing exposure of live
      // entries and the time since their last pulse.
      pulses = CacheBuilder.newBuilder()
          .expireAfterWrite(expiration.as(Time.MILLISECONDS), TimeUnit.MILLISECONDS)
          .<T, T>build()
          .asMap();
    }

    @Override
    public void pulse(T t) {
      pulses.put(t, t);
    }

    @Override
    public boolean isAlive(T t) {
      return pulses.containsKey(t);
    }

    @Override
    public Set<T> get() {
      return pulses.keySet();
    }
  }
}
