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
package org.apache.aurora.scheduler.storage.durability;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.scheduler.storage.durability.Persistence.Edit;
import org.apache.aurora.scheduler.storage.durability.Persistence.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to clone a persistence.
 */
final class Recovery {

  private static final Logger LOG = LoggerFactory.getLogger(Recovery.class);

  private Recovery() {
    // utility class.
  }

  /**
   * Copies all state from one persistence to another, batching into calls to
   * {@link Persistence#persist(Stream)}.
   *
   * @param from Source.
   * @param to Destination.
   * @param batchSize Maximum number of entries to include in any given persist.
   */
  static void copy(Persistence from, Persistence to, int batchSize) {
    requireEmpty(to);

    long start = System.nanoTime();
    AtomicLong count = new AtomicLong();
    AtomicInteger batchNumber = new AtomicInteger();
    List<Op> batch = Lists.newArrayListWithExpectedSize(batchSize);
    Runnable saveBatch = () -> {
      LOG.info("Saving batch " + batchNumber.incrementAndGet());
      try {
        to.persist(batch.stream());
      } catch (PersistenceException e) {
        throw new RuntimeException(e);
      }
      batch.clear();
    };

    AtomicBoolean dataBegin = new AtomicBoolean(false);
    try {
      from.recover()
          .filter(edit -> {
            if (edit.isDeleteAll()) {
              // Suppress any storage reset instructions.
              // Persistence implementations may recover with these, but do not support persisting
              // them.  As a result, we require that the recovery source produces a reset
              // instruction at the beginning of the stream, if at all.

              if (dataBegin.get()) {
                throw new IllegalStateException(
                    "A storage reset instruction arrived after the beginning of data");
              }
              return false;
            } else {
              dataBegin.set(true);
            }
            return true;
          })
          .forEach(edit -> {
            count.incrementAndGet();
            batch.add(edit.getOp());
            if (batch.size() == batchSize) {
              saveBatch.run();
              LOG.info("Fetching batch");
            }
          });
    } catch (PersistenceException e) {
      throw new RuntimeException(e);
    }

    if (!batch.isEmpty()) {
      saveBatch.run();
    }
    long end = System.nanoTime();
    LOG.info("Recovery finished");
    LOG.info("Copied " + count.get() + " ops in "
        + Amount.of(end - start, Time.NANOSECONDS).as(Time.MILLISECONDS) + " ms");
  }

  private static void requireEmpty(Persistence persistence) {
    LOG.info("Ensuring recovery destination is empty");
    try (Stream<Edit> edits = persistence.recover()) {
      if (edits.findFirst().isPresent()) {
        throw new IllegalStateException("Refusing to recover into non-empty persistence");
      }
    } catch (PersistenceException e) {
      throw new RuntimeException(e);
    }
  }
}
