/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.log.mesos;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.mesos.Log;
import org.apache.mesos.Log.Entry;
import org.apache.mesos.Log.Position;

/**
 * An interface for {@link Log}, since the mesos Java API doesn't provide one.
 * <p>
 * This is needed because a static initializer that loads a native library, prevents us from
 * mocking the Mesos Log API in tests. These wrapper interfaces and their corresponding
 * implementations that delegate the calls to the underlying Mesos Log objects will help
 * us mock the Log API.
 * <p>
 * TODO(Suman Karumuri): Remove this interface after https://issues.apache.org/jira/browse/MESOS-796
 * is resolved.
 */
interface LogInterface {

  Position position(byte[] identity);

  interface ReaderInterface {
    List<Entry> read(Position from, Position to, long timeout, TimeUnit unit)
        throws TimeoutException, Log.OperationFailedException;

    Position beginning();

    Position ending();
  }

  interface WriterInterface {
    Position append(byte[] data, long timeout, TimeUnit unit)
        throws TimeoutException, Log.WriterFailedException;

    Position truncate(Position to, long timeout, TimeUnit unit)
        throws TimeoutException, Log.WriterFailedException;
  }
}
