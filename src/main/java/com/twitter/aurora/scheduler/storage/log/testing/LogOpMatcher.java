/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.storage.log.testing;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.easymock.IExpectationSetters;

import com.twitter.aurora.codec.ThriftBinaryCodec;
import com.twitter.aurora.codec.ThriftBinaryCodec.CodingException;
import com.twitter.aurora.gen.storage.Constants;
import com.twitter.aurora.gen.storage.LogEntry;
import com.twitter.aurora.gen.storage.Op;
import com.twitter.aurora.gen.storage.Snapshot;
import com.twitter.aurora.gen.storage.Transaction;
import com.twitter.aurora.scheduler.log.Log.Position;
import com.twitter.aurora.scheduler.log.Log.Stream;

import static org.easymock.EasyMock.expect;

/**
 * A junit argument matcher that detects same-value {@link LogEntry} objects in a more human
 * readable way than byte array comparison.
 */
public class LogOpMatcher implements IArgumentMatcher {
  private final LogEntry expected;

  public LogOpMatcher(LogEntry expected) {
    this.expected = expected;
  }

  @Override
  public boolean matches(Object argument) {
    try {
      return expected.equals(ThriftBinaryCodec.decodeNonNull(LogEntry.class, (byte[]) argument));
    } catch (CodingException e) {
      return false;
    }
  }

  @Override
  public void appendTo(StringBuffer buffer) {
    buffer.append(expected);
  }

  /**
   * Creates a stream matcher that will set expectations on the provided {@code stream}.
   *
   * @param stream Mocked stream.
   * @return A stream matcher to set expectations against {@code stream}.
   */
  public static StreamMatcher matcherFor(Stream stream) {
    return new StreamMatcher(stream);
  }

  public static final class StreamMatcher {
    private final Stream stream;

    private StreamMatcher(Stream stream) {
      this.stream = Preconditions.checkNotNull(stream);
    }

    /**
     * Sets an expectation for a stream transaction containing the provided {@code ops}.
     *
     * @param ops Operations to expect in the transaction.
     * @return An expectation setter.
     */
    public IExpectationSetters<Position> expectTransaction(Op...ops) {
      LogEntry entry = LogEntry.transaction(
          new Transaction(ImmutableList.copyOf(ops), Constants.CURRENT_SCHEMA_VERSION));
      return expect(stream.append(sameEntry(entry)));
    }

    /**
     * Sets an expectation for a snapshot.
     *
     * @param snapshot Expected snapshot.
     * @return An expectation setter.
     */
    public IExpectationSetters<Position> expectSnapshot(Snapshot snapshot) {
      LogEntry entry = LogEntry.snapshot(snapshot);
      return expect(stream.append(sameEntry(entry)));
    }
  }

  /**
   * Creates a matcher that supports value matching between a serialized {@link LogEntry} byte array
   * and a log entry object.
   *
   * @param entry Entry to match against.
   * @return {@code null}, return value included for easymock-style embedding.
   */
  private static byte[] sameEntry(LogEntry entry) {
    EasyMock.reportMatcher(new LogOpMatcher(entry));
    return null;
  }
}
