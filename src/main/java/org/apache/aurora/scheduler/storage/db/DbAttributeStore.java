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

import java.util.Objects;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.entities.IAttribute;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;

import static com.twitter.common.inject.TimedInterceptor.Timed;

/**
 * Attribute store backed by a relational database.
 */
class DbAttributeStore implements AttributeStore.Mutable {

  private final AttributeMapper mapper;

  @Inject
  DbAttributeStore(AttributeMapper mapper) {
    this.mapper = Objects.requireNonNull(mapper);
  }

  @Override
  public void deleteHostAttributes() {
    mapper.truncate();
  }

  @Timed("attribute_store_save")
  @Override
  public void saveHostAttributes(IHostAttributes hostAttributes) {
    HostAttributes mutableAttributes = hostAttributes.newBuilder();

    // Default to NONE maintenance mode.
    if (!hostAttributes.isSetMode()) {
      mutableAttributes.setMode(MaintenanceMode.NONE);
    }
    // Ensure attributes is non-null.
    if (!hostAttributes.isSetAttributes()) {
      mutableAttributes.setAttributes(ImmutableSet.<Attribute>of());
    }

    // If this is an 'upsert', don't overwrite the previously-set maintenance mode.
    Optional<IHostAttributes> existing = getHostAttributes(hostAttributes.getHost());
    IHostAttributes toSave;
    if (existing.isPresent()) {
      mutableAttributes.setMode(existing.get().getMode());

      toSave = IHostAttributes.build(mutableAttributes);

      // Avoid inserting again if this is a no-op update.
      if (existing.get().equals(toSave)) {
        return;
      }
    } else {
      toSave = IHostAttributes.build(mutableAttributes);
    }

    merge(toSave);
  }

  private static final Predicate<IAttribute> EMPTY_VALUES = new Predicate<IAttribute>() {
    @Override
    public boolean apply(IAttribute attribute) {
      return attribute.getValues().isEmpty();
    }
  };

  private void merge(IHostAttributes hostAttributes) {
    if (Iterables.any(hostAttributes.getAttributes(), EMPTY_VALUES)) {
      throw new IllegalArgumentException(
          "Host attributes contains empty values: " + hostAttributes);
    }

    mapper.deleteAttributesAndValues(hostAttributes.getHost());
    mapper.insert(hostAttributes);
    if (!hostAttributes.getAttributes().isEmpty()) {
      mapper.insertAttributeValues(hostAttributes);
    }
  }

  @Timed("attribute_store_set_mode")
  @Override
  public boolean setMaintenanceMode(String host, MaintenanceMode mode) {
    Optional<IHostAttributes> existing = getHostAttributes(host);
    if (existing.isPresent()) {
      merge(IHostAttributes.build(existing.get().newBuilder().setMode(mode)));
      return true;
    } else {
      return false;
    }
  }

  @Timed("attribute_store_fetch_one")
  @Override
  public Optional<IHostAttributes> getHostAttributes(String host) {
    return Optional.fromNullable(mapper.select(host)).transform(IHostAttributes.FROM_BUILDER);
  }

  @Timed("attribute_store_fetch_all")
  @Override
  public Set<IHostAttributes> getHostAttributes() {
    return IHostAttributes.setFromBuilders(mapper.selectAll());
  }
}
