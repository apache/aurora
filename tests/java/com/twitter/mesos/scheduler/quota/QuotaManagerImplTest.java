package com.twitter.mesos.scheduler.quota;

import com.google.common.testing.junit4.TearDownTestCase;

import org.junit.Before;
import org.junit.Test;

import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.scheduler.quota.QuotaManager.InsufficientQuotaException;
import com.twitter.mesos.scheduler.quota.QuotaManager.QuotaManagerImpl;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult;
import com.twitter.mesos.scheduler.storage.db.DbStorageTestUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author William Farner
 */
public class QuotaManagerImplTest extends TearDownTestCase {

  private static final Quota NO_QUOTA = new Quota();

  private QuotaManager quotaManager;

  @Before
  public void setUp() throws Exception {
    Storage storage = DbStorageTestUtil.setupStorage(this);
    storage.start(new NoResult.Quiet() {
      @Override protected void execute(Storage.StoreProvider storeProvider) {}
    });
    quotaManager = new QuotaManagerImpl(storage);
  }

  @Test
  public void testGetEmptyQuota() {
    assertEquals(NO_QUOTA, quotaManager.getQuota("foo"));
  }

  @Test
  public void testSetQuota() {
    Quota quota = new Quota(2, 2, 2);
    quotaManager.setQuota("foo", quota);
    assertEquals(quota, quotaManager.getQuota("foo"));

    quota = new Quota(1, 1, 1);
    quotaManager.setQuota("foo", quota);
    assertEquals(quota, quotaManager.getQuota("foo"));
  }

  @Test
  public void testConsumeAndReleaseNoQuota() {
    quotaManager.consumeQuota("foo", NO_QUOTA);
    quotaManager.releaseQuota("foo", NO_QUOTA);
    assertEquals(NO_QUOTA, quotaManager.getQuota("foo"));
  }

  @Test(expected = InsufficientQuotaException.class)
  public void testNoQuotaExhausted() {
    quotaManager.consumeQuota("foo", new Quota(1, 1, 1));
  }

  @Test
  public void testUseAllQuota() {
    quotaManager.setQuota("foo", new Quota(2, 2, 2));
    quotaManager.consumeQuota("foo", new Quota(1, 1, 1));
    quotaManager.consumeQuota("foo", new Quota(1, 1, 1));
  }

  @Test(expected = InsufficientQuotaException.class)
  public void testExhaustCpu() {
    quotaManager.setQuota("foo", new Quota(3, 3, 3));
    quotaManager.consumeQuota("foo", new Quota(1, 1, 1));
    quotaManager.consumeQuota("foo", new Quota(3, 1, 1));
  }

  @Test(expected = InsufficientQuotaException.class)
  public void testExhaustRam() {
    quotaManager.setQuota("foo", new Quota(3, 3, 3));
    quotaManager.consumeQuota("foo", new Quota(1, 1, 1));
    quotaManager.consumeQuota("foo", new Quota(1, 3, 1));
  }

  @Test(expected = InsufficientQuotaException.class)
  public void testExhaustDisk() {
    quotaManager.setQuota("foo", new Quota(3, 3, 3));
    quotaManager.consumeQuota("foo", new Quota(1, 1, 1));
    quotaManager.consumeQuota("foo", new Quota(1, 1, 3));
  }

  @Test
  public void testRelease() {
    quotaManager.setQuota("foo", new Quota(2, 2, 2));
    quotaManager.consumeQuota("foo", new Quota(1, 1, 1));
    quotaManager.consumeQuota("foo", new Quota(1, 1, 1));

    try {
      quotaManager.consumeQuota("foo", new Quota(1, 1, 1));
      fail("Quota should have been exhausted.");
    } catch (InsufficientQuotaException e) {
      // Expected.
    }

    quotaManager.releaseQuota("foo", new Quota(1, 1, 1));
    quotaManager.consumeQuota("foo", new Quota(1, 1, 1));
  }

  @Test(expected = IllegalStateException.class)
  public void testBadRelease() {
    quotaManager.releaseQuota("foo", new Quota(1, 1, 1));
  }

  @Test(expected = IllegalStateException.class)
  public void testBadRelease2() {
    quotaManager.setQuota("foo", new Quota(2, 2, 2));
    quotaManager.consumeQuota("foo", new Quota(1, 1, 1));
    quotaManager.releaseQuota("foo", new Quota(2, 2, 2));
  }
}
