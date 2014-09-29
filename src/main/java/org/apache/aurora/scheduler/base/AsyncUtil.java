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
package org.apache.aurora.scheduler.base;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static java.util.Objects.requireNonNull;

/**
 * Utility class for facilitating async scheduling.
 */
public final class AsyncUtil {

  private AsyncUtil() {
    // Utility class.
  }

  /**
   * Creates a {@link ScheduledThreadPoolExecutor} that logs unhandled errors.
   *
   * @param poolSize Thread pool size.
   * @param nameFormat Thread naming format.
   * @param logger Logger instance.
   * @return instance of {@link ScheduledThreadPoolExecutor} enabled to log unhandled exceptions.
   */
  public static ScheduledThreadPoolExecutor loggingScheduledExecutor(
      int poolSize,
      String nameFormat,
      final Logger logger) {

    requireNonNull(nameFormat);

    return new ScheduledThreadPoolExecutor(
        poolSize,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build()) {

      @Override
      protected void afterExecute(Runnable runnable, Throwable throwable) {
        // See java.util.concurrent.ThreadPoolExecutor#afterExecute(Runnable, Throwable)
        // for more details and an implementation example.
        super.afterExecute(runnable, throwable);
        if (throwable == null) {
          if (runnable instanceof Future) {
            try {
              Future<?> future = (Future<?>) runnable;
              if (future.isDone()) {
                future.get();
              }
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            } catch (ExecutionException ee) {
              logger.log(Level.SEVERE, ee.toString(), ee);
            }
          }
        } else {
          logger.log(Level.SEVERE, throwable.toString(), throwable);
        }
      }
    };
  }

  /**
   * Creates a single-threaded {@link ScheduledThreadPoolExecutor} that logs unhandled errors.
   *
   * @param nameFormat Thread naming format.
   * @param logger Logger instance.
   * @return instance of {@link ScheduledThreadPoolExecutor} enabled to log unhandled exceptions.
   */
  public static ScheduledThreadPoolExecutor singleThreadLoggingScheduledExecutor(
      String nameFormat,
      Logger logger) {

    return loggingScheduledExecutor(1, nameFormat, logger);
  }
}
