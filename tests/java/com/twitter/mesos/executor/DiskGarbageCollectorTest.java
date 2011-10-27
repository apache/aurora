package com.twitter.mesos.executor;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import org.apache.commons.io.FileUtils;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.or;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author William Farner
 */
public class DiskGarbageCollectorTest {

  private File root;
  private File fileA;
  private File fileB;
  private File fileC;

  private static final Amount<Long, Data> GC_THRESHOLD = Amount.of(10L, Data.KB);

  private IMocksControl control;
  private FileFilter fileFilter;
  private Closure<File> gcCallback;
  private DiskGarbageCollector gc;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    root = Files.createTempDir();

    fileA = new File(root, "a");
    fileB = new File(root, "b");
    fileC = new File(root, "c");

    control = createControl();
    fileFilter = control.createMock(FileFilter.class);
    gcCallback = control.createMock(Closure.class);
    gc = new DiskGarbageCollector("Test", root, fileFilter, GC_THRESHOLD, gcCallback);
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
    gcCallback.execute(or(eq(fileA), or(eq(fileB), eq(fileC))));

    control.replay();

    populateFile(fileA, Amount.of(5, Data.KB));
    populateFile(fileB, Amount.of(5, Data.KB));
    populateFile(fileC, Amount.of(5, Data.KB));
    gc.run();

    assertThat(root.exists(), is(true));
    assertThat(root.isDirectory(), is(true));
    Set<String> dirContents = Sets.newHashSet(root.list());
    assertThat(dirContents.size(), is(2));

    // Since all three files were essentially populated simultaneously, it's uncertain which
    // one will be determined to be oldest by modification time.
    assertThat(Sets.intersection(Sets.newHashSet("a", "b", "c"), dirContents).size(), is(2));
  }

  @Test
  public void testReclaimsOldestFirst() throws Exception {
    expect(fileFilter.accept(fileB)).andReturn(true);
    expect(fileFilter.accept(fileA)).andReturn(true);
    expect(fileFilter.accept(fileC)).andReturn(true);
    gcCallback.execute(fileB);

    control.replay();

    populateFile(fileB, Amount.of(5, Data.KB));
    Thread.sleep(1000);  // Needed to guarantee clock moves.
    populateFile(fileA, Amount.of(3, Data.KB));
    populateFile(fileC, Amount.of(3, Data.KB));
    gc.run();

    assertDirContents(root, "a", "c");
  }

  @Test
  public void testIgnoresFilteredFiles() throws Exception {
    expect(fileFilter.accept(fileA)).andReturn(true);
    expect(fileFilter.accept(fileB)).andReturn(true);
    expect(fileFilter.accept(fileC)).andReturn(false);
    gcCallback.execute(fileA);
    gcCallback.execute(fileB);

    control.replay();

    populateFile(fileA, Amount.of(11, Data.KB));
    populateFile(fileB, Amount.of(11, Data.KB));
    populateFile(fileC, Amount.of(11, Data.KB));
    gc.run();

    assertDirContents(root, "c");
  }

  @Test
  public void testRecursiveGc() throws Exception {
    File fileA1 = new File(fileA, "1");
    File fileA2 = new File(fileA, "2");

    expect(fileFilter.accept(fileB)).andReturn(true);
    expect(fileFilter.accept(fileC)).andReturn(true);
    expect(fileFilter.accept(fileA)).andReturn(true);

    control.replay();

    populateFile(fileB, Amount.of(4, Data.KB));
    populateFile(fileC, Amount.of(5, Data.KB));
    assertThat(fileA.mkdir(), is(true));
    populateFile(fileA1, Amount.of(10, Data.KB));
    populateFile(fileA2, Amount.of(10, Data.KB));

    gc.run();

    assertTrue(FileUtils.sizeOfDirectory(root) < GC_THRESHOLD.as(Data.BYTES));
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
