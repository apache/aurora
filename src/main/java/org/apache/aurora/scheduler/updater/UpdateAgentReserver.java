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
package org.apache.aurora.scheduler.updater;

import java.util.Optional;

import com.google.inject.Inject;

import org.apache.aurora.scheduler.preemptor.BiCache;
import org.apache.aurora.scheduler.storage.entities.IInstanceKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Reserves agents for instances being updated. Multiple instance keys can be registered against
 * a single agent.
 */
public interface UpdateAgentReserver {

  /**
   * Reserves the agent id for the given key. Should behave like a multi-map under the hood.
   *
   * @param agentId The agent id to reserve.
   * @param key The instance key that will use the reservation.
   */
  void reserve(String agentId, IInstanceKey key);

  /**
   * Releases the reservation on an agent id for the given key.
   *
   * @param agentId The agent id to release the reservation on.
   * @param key The instance key that should be removed.
   */
  void release(String agentId, IInstanceKey key);

  /**
   * Returns the agent id associated with the given instance key.
   *
   * @param key The instance key to look up.
   * @return An optional agent id string.
   */
  Optional<String> getAgent(IInstanceKey key);

  /**
   * Checks whether an agent is currently reserved. Useful for skipping over the agent between the
   * reserve/release window.
   *
   * @param agentId The agent id to look up reservations for.
   * @return A set of keys reserved for that agent.
   */
  boolean isReserved(String agentId);

  /**
   * Implementation of the update reserver backed by a BiCache (the same mechanism we use for
   * preemption). This means it will expire reservations that haven't been explicitly released
   * after the configured timeout.
   */
  class UpdateAgentReserverImpl implements UpdateAgentReserver {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateAgentReserverImpl.class);

    private final BiCache<IInstanceKey, String> cache;

    @Inject
    UpdateAgentReserverImpl(BiCache<IInstanceKey, String> cache) {
      this.cache = requireNonNull(cache);
    }

    public void reserve(String agentId, IInstanceKey key) {
      LOG.info("Reserving {} for {}", agentId, key);
      cache.put(key, agentId);
    }

    public void release(String agentId, IInstanceKey key) {
      LOG.info("Releasing reservation on {} for {}", agentId, key);
      cache.remove(key, agentId);
    }

    @Override
    public boolean isReserved(String agentId) {
      return !cache.getByValue(agentId).isEmpty();
    }

    @Override
    public Optional<String> getAgent(IInstanceKey key) {
      return cache.get(key);
    }
  }

  /**
   * Used to effectively disable reservations.
   */
  class NullAgentReserver implements UpdateAgentReserver {
    @Override
    public void reserve(String agentId, IInstanceKey key) {
      // noop
    }

    @Override
    public void release(String agentId, IInstanceKey key) {
      // noop
    }

    @Override
    public Optional<String> getAgent(IInstanceKey key) {
      return Optional.empty();
    }

    @Override
    public boolean isReserved(String agentId) {
      return false;
    }
  }
}
