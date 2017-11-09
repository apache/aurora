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
package org.apache.aurora.scheduler.log.mesos;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.log.Log.Entry;
import org.apache.aurora.scheduler.log.Log.Stream.StreamAccessException;
import org.apache.aurora.scheduler.log.mesos.LogInterface.ReaderInterface;
import org.apache.aurora.scheduler.log.mesos.LogInterface.WriterInterface;
import org.apache.mesos.Log;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.log.mesos.MesosLog.LogStream.LogPosition;
import static org.apache.mesos.Log.Position;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MesosLogTest extends EasyMockTest {

  private static final Amount<Long, Time> READ_TIMEOUT = Amount.of(5L, Time.SECONDS);
  private static final Amount<Long, Time> WRITE_TIMEOUT = Amount.of(3L, Time.SECONDS);
  private static final String DUMMY_CONTENT = "test data";

  private Command shutdownHooks;
  private LogInterface backingLog;
  private ReaderInterface logReader;
  private WriterInterface logWriter;
  private org.apache.aurora.scheduler.log.Log.Stream logStream;

  @Before
  public void setUp() {
    shutdownHooks = createMock(Command.class);
    backingLog = createMock(LogInterface.class);
    logReader = createMock(ReaderInterface.class);
    logWriter = createMock(WriterInterface.class);

    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogInterface.class).toInstance(backingLog);
        bind(ReaderInterface.class).toInstance(logReader);
        bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(MesosLog.ReadTimeout.class)
            .toInstance(READ_TIMEOUT);
        bind(WriterInterface.class).toInstance(logWriter);
        bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(MesosLog.WriteTimeout.class)
            .toInstance(WRITE_TIMEOUT);
        bind(byte[].class).annotatedWith(MesosLog.NoopEntry.class)
            .toInstance(DUMMY_CONTENT.getBytes(StandardCharsets.UTF_8));
        bind(Lifecycle.class).toInstance(new Lifecycle(shutdownHooks));
      }
    });

    MesosLog log = injector.getInstance(MesosLog.class);
    logStream = log.open();
  }

  @Test
  public void testLogStreamTimeout() throws Exception {
    try {
      testMutationFailure(new TimeoutException("Task timed out"));
      fail();
    } catch (StreamAccessException e) {
      // Expected.
    }

    expectStreamUnusable();
  }

  @Test
  public void testLogStreamWriteFailure() throws Exception {
    try {
      testMutationFailure(new Log.WriterFailedException("Failed to write to log"));
      fail();
    } catch (StreamAccessException e) {
      // Expected.
    }

    expectStreamUnusable();
  }

  private void testMutationFailure(Exception e) throws Exception {
    String data = "hello";
    expectWrite(data).andThrow(e);
    shutdownHooks.execute();

    control.replay();
    logStream.append(data.getBytes(StandardCharsets.UTF_8));
  }

  private void expectStreamUnusable() throws Exception {
    try {
      logStream.append("nothing".getBytes(StandardCharsets.UTF_8));
      fail();
    } catch (IllegalStateException e) {
      // Expected.
    }
  }

  private static Position makePosition(long value) throws Exception {
    // The only way to create a Position instance is through a private constructor (MESOS-1519).
    Constructor<Position> positionConstructor = Position.class.getDeclaredConstructor(long.class);
    positionConstructor.setAccessible(true);
    return positionConstructor.newInstance(value);
  }

  private static Log.Entry makeEntry(Position position, String data) throws Exception {
    // The only way to create an Entry instance is through a private constructor (MESOS-1519).
    Constructor<Log.Entry> entryConstructor =
        Log.Entry.class.getDeclaredConstructor(Position.class, byte[].class);
    entryConstructor.setAccessible(true);
    return entryConstructor.newInstance(position, data.getBytes(StandardCharsets.UTF_8));
  }

  private IExpectationSetters<Position> expectWrite(String content) throws Exception {
    return expect(
        logWriter.append(EasyMock.aryEq(content.getBytes(StandardCharsets.UTF_8)),
            // Cast is needed to prevent NullPointerException on unboxing.
            EasyMock.eq((long) WRITE_TIMEOUT.getValue()),
            EasyMock.eq(WRITE_TIMEOUT.getUnit().getTimeUnit())));
  }

  private Position expectWrite(String content, long resultingPosition) throws Exception {
    Position position = makePosition(resultingPosition);
    expectWrite(content).andReturn(position);
    return position;
  }

  private void expectDiscoverEntryRange(Position beginning, Position end) {
    expect(logReader.beginning()).andReturn(beginning);
    expect(logReader.ending()).andReturn(end);
  }

  private void expectSetPosition(Position position) {
    expect(backingLog.position(EasyMock.aryEq(position.identity()))).andReturn(position);
  }

  private IExpectationSetters<List<Log.Entry>> expectRead(Position position) throws Exception {
    expectSetPosition(position);
    return expect(logReader.read(
        position,
        position,
        READ_TIMEOUT.getValue(),
        READ_TIMEOUT.getUnit().getTimeUnit()));
  }

  private void expectRead(Position position, String dataReturned) throws Exception {
    expectRead(position).andReturn(ImmutableList.of(makeEntry(position, dataReturned)));
  }

  private List<String> readAll() {
    List<byte[]> entryBytes = FluentIterable.from(ImmutableList.copyOf(logStream.readAll()))
        .transform(Entry::contents)
        .toList();
    return FluentIterable.from(entryBytes)
        .transform(data -> new String(data, StandardCharsets.UTF_8))
        .toList();
  }

  @Test
  public void testLogRead() throws Exception {
    Position beginning = makePosition(1);
    Position middle = makePosition(2);
    Position end = expectWrite(DUMMY_CONTENT, 3);
    expectDiscoverEntryRange(beginning, end);
    String beginningData = "beginningData";
    String middleData = "middleData";
    expectRead(beginning, beginningData);
    expectRead(middle, middleData);
    expectRead(end, DUMMY_CONTENT);
    String newData = "newly appended data";
    expectWrite(newData, 4);

    control.replay();

    assertEquals(ImmutableList.of(beginningData, middleData, DUMMY_CONTENT), readAll());
    logStream.append(newData.getBytes());

  }

  @Test(expected = StreamAccessException.class)
  public void testInitialAppendFails() throws Exception {
    expectWrite(DUMMY_CONTENT).andThrow(new Log.WriterFailedException("injected"));
    shutdownHooks.execute();

    control.replay();

    readAll();
  }

  @Test(expected = StreamAccessException.class)
  public void testReadTimeout() throws Exception {
    Position beginning = makePosition(1);
    Position end = expectWrite(DUMMY_CONTENT, 3);
    expectDiscoverEntryRange(beginning, end);
    expectRead(beginning).andThrow(new TimeoutException("injected"));

    control.replay();

    readAll();
  }

  @Test(expected = StreamAccessException.class)
  public void testLogError() throws Exception {
    Position beginning = makePosition(1);
    Position end = expectWrite(DUMMY_CONTENT, 3);
    expectDiscoverEntryRange(beginning, end);
    expectRead(beginning).andThrow(new Log.OperationFailedException("injected"));

    control.replay();

    readAll();
  }

  @Test
  public void testTruncate() throws Exception {
    Position truncate = makePosition(5);
    expect(logWriter.truncate(
        truncate,
        WRITE_TIMEOUT.getValue(),
        WRITE_TIMEOUT.getUnit().getTimeUnit()))
        .andReturn(truncate);

    control.replay();

    logStream.truncateBefore(new LogPosition(truncate));
  }

  @Test(expected = NoSuchElementException.class)
  public void testIteratorUsage() throws Exception {
    Position beginning = makePosition(1);
    Position middle = makePosition(2);
    Position end = expectWrite(DUMMY_CONTENT, 3);
    expectDiscoverEntryRange(beginning, end);
    // SKipped entries.
    expectRead(beginning).andReturn(ImmutableList.of());
    expectRead(middle).andReturn(ImmutableList.of());
    expectRead(end).andReturn(ImmutableList.of());

    control.replay();

    // So close!  The implementation requires that hasNext() is called first.
    logStream.readAll().next();
  }
}
