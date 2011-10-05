package com.twitter.mesos.scheduler.log.db;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.testing.TearDown;
import com.google.common.testing.junit4.TearDownTestCase;
import com.twitter.mesos.scheduler.db.DbUtil.DbAccess;
import com.twitter.mesos.scheduler.db.testing.DbTestUtil;
import com.twitter.mesos.scheduler.log.Log;
import com.twitter.mesos.scheduler.log.Log.Entry;
import com.twitter.mesos.scheduler.log.Log.Position;
import com.twitter.mesos.scheduler.log.Log.Stream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import static com.google.common.testing.junit4.JUnitAsserts.assertContentsInOrder;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * @author John Sirois
 */
public class DbLogStreamTest extends TearDownTestCase {

  private Log log;

  @Before
  public void setUp() throws SQLException {
    DbAccess dbAccess = DbTestUtil.setupStorage(this);
    log = new DbLogStream(dbAccess.transactionTemplate, dbAccess.jdbcTemplate);
  }

  @Test
  public void testReadAfter_empty() throws IOException {
    Stream stream = openStream();
    assertContents(stream.readFrom(stream.beginning()));
    assertContents(stream.readFrom(stream.end()));
  }

  @Test
  public void testTruncateBefore_empty() throws Exception {
    Stream stream = openStream();
    stream.truncateBefore(stream.beginning());
    assertContents(stream.readFrom(stream.beginning()));

    stream.truncateBefore(stream.end());
    assertContents(stream.readFrom(stream.beginning()));
  }

  @Test
  public void testSize_empty() throws IOException {
    Stream stream = openStream();
    assertFalse(stream.readFrom(stream.beginning()).hasNext());
  }

  @Test
  public void testAppend() throws IOException {
    Stream stream = openStream();
    assertContents(stream.readFrom(stream.beginning()));

    Position joe = stream.append("joe".getBytes());
    assertContents(stream.readFrom(stream.beginning()), "joe");

    stream.append("bob".getBytes());
    stream.append("jane".getBytes());
    assertContents(stream.readFrom(joe), "joe", "bob", "jane");
  }

  private static final Function<Entry,Position> GET_POSITION = new Function<Entry, Position>() {
    @Override public Position apply(Entry entry) {
      return entry.position();
    }
  };

  @Test
  public void testOrdering() throws IOException {
    Stream stream = openStream();

    Position c = stream.append("c".getBytes());
    Position b = stream.append("b".getBytes());
    Position a = stream.append("a".getBytes());

    List<Entry> entries = Lists.newArrayList(stream.readFrom(stream.beginning()));
    assertContents(entries.iterator(), "c", "b", "a");

    ImmutableList<Position> sortedPositions =
        Ordering.natural().immutableSortedCopy(Lists.transform(entries, GET_POSITION));
    assertContentsInOrder(sortedPositions, c, b, a);
  }

  private void assertContents(Iterator<Entry> actual, String... expected) {
    ImmutableList<Entry> actualEntries = ImmutableList.copyOf(actual);
    assertEquals(expected.length, actualEntries.size());

    int i = 0;
    for (Entry entry : actualEntries) {
      assertTrue(
          String.format("Too many entries present; expected %d got: %s",
              expected.length, actualEntries),
          i < expected.length);
      Assert.assertArrayEquals(entry.contents(), expected[i++].getBytes());
    }
  }

  private Stream openStream() throws IOException {
    final Stream stream = log.open();
    addTearDown(new TearDown() {
      @Override public void tearDown() throws Exception {
        stream.close();
      }
    });
    return stream;
  }
}
