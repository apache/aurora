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
package org.apache.aurora;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collector;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;
import com.google.common.util.concurrent.ServiceManager;

import static java.util.stream.Collector.Characteristics.UNORDERED;

/**
 * Utilities for working with Guava.
 */
public final class GuavaUtils {
  private GuavaUtils() {
    // Utility class.
  }

  /**
   * Collector to create a Guava ImmutableSet.
   */
  public static <T> Collector<T, ?, ImmutableSet<T>> toImmutableSet() {
    return Collector.of(
        ImmutableSet::builder,
        ImmutableSet.Builder::add,
        (ImmutableSet.Builder<T> l, ImmutableSet.Builder<T> r) -> l.addAll(r.build()),
        ImmutableSet.Builder::build,
        UNORDERED);
  }

  /**
   * Interface for mocking. The Guava ServiceManager class is final.
   */
  public interface ServiceManagerIface {
    ServiceManagerIface startAsync();

    void awaitHealthy();

    ServiceManagerIface stopAsync();

    void awaitStopped(long timeout, TimeUnit unit) throws TimeoutException;

    ImmutableMultimap<State, Service> servicesByState();
  }

  /**
   * Create a new {@link ServiceManagerIface} that wraps a {@link ServiceManager}.
   *
   * @param delegate Service manager to delegate to.
   * @return A wrapper.
   */
  public static ServiceManagerIface serviceManager(final ServiceManager delegate) {
    return new ServiceManagerIface() {
      @Override
      public ServiceManagerIface startAsync() {
        delegate.startAsync();
        return this;
      }

      @Override
      public void awaitHealthy() {
        delegate.awaitHealthy();
      }

      @Override
      public ServiceManagerIface stopAsync() {
        delegate.stopAsync();
        return this;
      }

      @Override
      public void awaitStopped(long timeout, TimeUnit unit) throws TimeoutException {
        delegate.awaitStopped(timeout, unit);
      }

      @Override
      public ImmutableMultimap<State, Service> servicesByState() {
        return delegate.servicesByState();
      }
    };
  }
}
