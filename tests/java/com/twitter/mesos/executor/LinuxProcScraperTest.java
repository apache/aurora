package com.twitter.mesos.executor;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * @author wfarner
 */
public class LinuxProcScraperTest {

  private static final String TEST_STAT_DATA =
      "61732 (java) S 61729 24709 24709 0 -1 4202496 215829 0 1 0 1025 67 0 0 18 0 32 0"
      + " 555440637 2520588288 206461 18446744073709551615 1073741824 1073778376 140733778372544"
      + " 18446744073709551615 233027171925 0 4 0 16800975 18446744073709551615 0 0 17 14 0 0 0";

  private LinuxProcScraper scraper;

  @Before
  public void setUp() {
    scraper = new LinuxProcScraper(new File("/"));
  }

  @Test
  public void testEmptyString() {
    assertNull(scraper.parse(""));
  }

  @Test
  public void testTooFewFields() {
    assertNull(scraper.parse("asdf"));
  }

  @Test
  public void testInvalidNumbers() {
    List<String> fields = Lists.newArrayList();
    for (int i = 0; i < 50; i++) {
      fields.add("foo");
    }

    assertNull(scraper.parse(Joiner.on(" ").join(fields)));
  }

  @Test
  public void testGoodData() {
    LinuxProcScraper.ProcInfo info = scraper.parse(TEST_STAT_DATA).build();
    assertThat(info.getVmSize().as(Data.BYTES),
        is(Amount.of(2520588288L, Data.BYTES).as(Data.BYTES)));
    assertThat(info.getNiceLevel(), is(0));
    assertThat(info.getJiffiesUsed(), is(1025L + 67L));
  }
}
