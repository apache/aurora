package com.twitter.mesos.executor;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.testing.EasyMockTest;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.or;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author William Farner
 */
public class DiskGarbageCollectorTest extends EasyMockTest {

  private File root;
  private File fileA;
  private File fileB;
  private File fileC;

  private static final Amount<Long, Data> GC_THRESHOLD = Amount.of(10L, Data.KB);

  private FileFilter fileFilter;
  private Closure<Set<File>> gcCallback;
  private DiskGarbageCollector gc;

  @Before
  public void setUp() {
    root = Files.createTempDir();

    fileA = new File(root, "a");
    fileB = new File(root, "b");
    fileC = new File(root, "c");

    fileFilter = createMock(FileFilter.class);
    gcCallback = createMock(new Clazz<Closure<Set<File>>>() {});
    gc = new DiskGarbageCollector("Test",
        root,
        fileFilter,
        GC_THRESHOLD,
        gcCallback);
  }

  @After
  public void verifyControl() {
    control.verify();
    try {
      FileUtils.deleteDirectory(root);
    } catch (IOException e) {
      // No-op.
    }
  }

  @Test
  public void testNoGcNeeded() {
    control.replay();

    gc.run();
  }

  @Test
  public void testSimpleGc() throws Exception {
    expect(fileFilter.accept(fileA)).andReturn(true);
    expect(fileFilter.accept(fileB)).andReturn(true);
    expect(fileFilter.accept(fileC)).andReturn(true);
    gcCallback.execute(
        or(eq(ImmutableSet.of(fileA)), or(eq(ImmutableSet.of(fileB)), eq(ImmutableSet.of(fileC)))));

    control.replay();

    populateFile(fileA, Amount.of(5, Data.KB));
    populateFile(fileB, Amount.of(5, Data.KB));
    populateFile(fileC, Amount.of(5, Data.KB));
    gc.run();
  }

  @Test
  public void testIgnoresFilteredFiles() throws Exception {
    expect(fileFilter.accept(fileA)).andReturn(true);
    expect(fileFilter.accept(fileB)).andReturn(true);
    expect(fileFilter.accept(fileC)).andReturn(false);
    gcCallback.execute(ImmutableSet.of(fileA, fileB));

    control.replay();

    populateFile(fileA, Amount.of(11, Data.KB));
    populateFile(fileB, Amount.of(11, Data.KB));
    populateFile(fileC, Amount.of(11, Data.KB));
    gc.run();
  }

  @Test
  public void testRecursiveGc() throws Exception {
    File fileA1 = new File(fileA, "1");
    File fileA2 = new File(fileA, "2");

    expect(fileFilter.accept(fileB)).andReturn(true);
    expect(fileFilter.accept(fileC)).andReturn(true);
    expect(fileFilter.accept(fileA)).andReturn(true);

    // At least one of the 3 top-level items will need to be gc'd
    gcCallback.execute(EasyMock.<Set<File>>anyObject());
    expectLastCall().atLeastOnce();

    control.replay();

    populateFile(fileB, Amount.of(4, Data.KB));
    populateFile(fileC, Amount.of(5, Data.KB));
    assertThat(fileA.mkdir(), is(true));
    populateFile(fileA1, Amount.of(10, Data.KB));
    populateFile(fileA2, Amount.of(10, Data.KB));

    gc.run();
  }

  private void populateFile(File file, Amount<Integer, Data> bytes) throws Exception {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < bytes.as(Data.BYTES); i++) {
      sb.append("A");
    }

    Files.write(sb, file, Charsets.US_ASCII);
  }

  private void assertDirContents(File dir, String... children) {
    assertThat(dir.exists(), is(true));
    assertThat(dir.isDirectory(), is(true));
    assertThat(Sets.newHashSet(dir.list()), is(Sets.newHashSet(children)));
  }
}
