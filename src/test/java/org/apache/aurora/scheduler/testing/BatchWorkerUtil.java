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
package org.apache.aurora.scheduler.testing;

import java.util.concurrent.CompletableFuture;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.BatchWorker;
import org.apache.aurora.scheduler.BatchWorker.Work;
import org.apache.aurora.scheduler.storage.Storage;
import org.easymock.Capture;
import org.easymock.IExpectationSetters;
import org.easymock.IMocksControl;

import static org.apache.aurora.common.testing.easymock.EasyMockTest.createCapture;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;

public final class BatchWorkerUtil {
  private BatchWorkerUtil() {
    // Utility class.
  }

  public static <T> IExpectationSetters<CompletableFuture<T>> expectBatchExecute(
      BatchWorker<T> batchWorker,
      Storage storage,
      IMocksControl control,
      T resultValue) throws Exception {

    final CompletableFuture<T> result = new EasyMockTest.Clazz<CompletableFuture<T>>() { }
        .createMock(control);
    expect(result.get()).andReturn(resultValue).anyTimes();

    final Capture<Work<T>> capture = createCapture();
    return expect(batchWorker.execute(capture(capture))).andAnswer(() -> {
      storage.write((Storage.MutateWork.NoResult.Quiet) store -> capture.getValue().apply(store));
      return result;
    });
  }

  public static <T> IExpectationSetters<CompletableFuture<T>> expectBatchExecute(
      BatchWorker<T> batchWorker,
      Storage storage,
      IMocksControl control) throws Exception {

    return expectBatchExecute(batchWorker, storage, control, null);
  }
}
