package com.twitter.mesos.scheduler.storage.mem;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.testing.TearDown;
import com.google.common.testing.junit4.TearDownTestCase;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * TODO(William Farner): Wire a mechanism to allow verification of synchronized writers.
 */
public class MemStorageTest extends TearDownTestCase {

  private ExecutorService executor;
  private Storage storage;

  @Before
  public void setUp() {
    executor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("SlowRead-%d").setDaemon(true).build());
    addTearDown(new TearDown() {
      @Override public void tearDown() {
        new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
      }
    });
    storage = MemStorage.newEmptyStorage();
  }

  @Test
  public void testConcurrentReaders() throws Exception {
    // Validate that a slow read does not block another read.

    final CountDownLatch slowReadStarted = new CountDownLatch(1);
    final CountDownLatch slowReadFinished = new CountDownLatch(1);

    Future<String> future = executor.submit(new Callable<String>() {
      @Override public String call() throws Exception {
        return storage.doInTransaction(new Work.Quiet<String>() {
          @Override public String apply(StoreProvider storeProvider) {
            slowReadStarted.countDown();
            try {
              slowReadFinished.await();
            } catch (InterruptedException e) {
              fail(e.getMessage());
            }
            return "slowResult";
          }
        });
      }
    });

    slowReadStarted.await();

    String fastResult = storage.doInTransaction(new Work.Quiet<String>() {
      @Override public String apply(StoreProvider storeProvider) {
        return "fastResult";
      }
    });
    assertEquals("fastResult", fastResult);
    slowReadFinished.countDown();
    assertEquals("slowResult", future.get());
  }
}
