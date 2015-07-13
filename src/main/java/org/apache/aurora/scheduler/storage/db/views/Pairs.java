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
package org.apache.aurora.scheduler.storage.db.views;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.twitter.common.collections.Pair;

import org.apache.aurora.GuavaUtils;

/**
 * Utility class for translating collections of {@link Pair} to and from maps.
 */
public final class Pairs {

  private Pairs() {
    // Utility class.
  }

  public static <K, V> Map<K, V> toMap(Iterable<Pair<K, V>> pairs) {
    ImmutableMap.Builder<K, V> map = ImmutableMap.builder();
    for (Pair<K, V> pair : pairs) {
      map.put(pair.getFirst(), pair.getSecond());
    }
    return map.build();
  }

  public static <K, V> List<Pair<K, V>> fromMap(Map<K, V> map) {
    return map.entrySet().stream()
        .map((e) -> Pair.of(e.getKey(), e.getValue()))
        .collect(GuavaUtils.toImmutableList());
  }
}
