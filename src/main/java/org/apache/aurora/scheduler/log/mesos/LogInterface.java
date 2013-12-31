package com.twitter.aurora.scheduler.log.mesos;

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
