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
package org.apache.aurora.scheduler.storage.db;

import com.google.inject.AbstractModule;
import com.twitter.common.inject.Bindings.KeyFactory;

import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;

import static java.util.Objects.requireNonNull;

/**
 * Temporary module to wire the two partial storage implementations together as we
 * migrate from MemStorage to DbStorage.  This accepts two {@link KeyFactory}s,
 * one that references the binding scope for the feature-complete write-behind
 * volatile storage system, and one for the binding scope of the new and partially-implemented
 * storage system.
 * <p>
 *  Once the new storage system is feature-complete, this module will be deleted
 *  as the binding bridge is no longer necessary.
 * </p>
 */
public class MigrationModule extends AbstractModule {

  private final KeyFactory toFactory;
  private final KeyFactory fromFactory;

  public MigrationModule(KeyFactory from, KeyFactory to) {
    this.fromFactory = requireNonNull(from);
    this.toFactory = requireNonNull(to);
  }

  private <T> void link(Class<T> clazz) {
    bind(fromFactory.create(clazz)).to(toFactory.create(clazz));
  }

  @Override
  protected void configure() {
    link(AttributeStore.Mutable.class);
    link(LockStore.Mutable.class);
    link(QuotaStore.Mutable.class);
    link(SchedulerStore.Mutable.class);
    link(JobUpdateStore.Mutable.class);
  }
}
