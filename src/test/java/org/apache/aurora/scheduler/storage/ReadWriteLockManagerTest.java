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
package org.apache.aurora.scheduler.storage;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.testing.TearDown;
import com.google.common.testing.junit4.TearDownTestCase;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadWriteLockManagerTest extends TearDownTestCase {

  private ReadWriteLockManager lockManager;
  private ExecutorService executor;

  @Before
  public void setUp() {
    lockManager = new ReadWriteLockManager();
    executor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("LockManagerTest-%d").setDaemon(true).build());
    addTearDown(new TearDown() {
      @Override
      public void tearDown() {
        new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
      }
    });
  }

  @Test
  public void testModeDowngrade() {
    lockManager.writeLock();
    lockManager.readLock();
  }

  @Test(expected = IllegalStateException.class)
  public void testModeUpgrade() {
    lockManager.readLock();
    lockManager.writeLock();
  }

  @Test
  public void testSimultaneousReads() throws Exception {
    final CountDownLatch slowReadStarted = new CountDownLatch(1);
    final CountDownLatch fastReadFinished = new CountDownLatch(1);

    Future<String> slowReadResult = executor.submit(new Callable<String>() {
      @Override public String call() throws Exception {
        lockManager.readLock();
        slowReadStarted.countDown();
        fastReadFinished.await();
        lockManager.readUnlock();
        return "slow";
      }
    });

    slowReadStarted.await();
    lockManager.readLock();
    lockManager.readUnlock();
    fastReadFinished.countDown();
    assertEquals("slow", slowReadResult.get());
  }

  @Test
  public void testReentrantReadLock() {
    assertTrue(lockManager.readLock());
    assertFalse(lockManager.readLock());
    lockManager.readUnlock();
    lockManager.readUnlock();
    assertTrue(lockManager.readLock());
  }

  @Test
  public void testReentrantWriteLock() {
    assertTrue(lockManager.writeLock());
    assertFalse(lockManager.writeLock());
    lockManager.writeUnlock();
    lockManager.writeUnlock();
    assertTrue(lockManager.writeLock());
    assertFalse(lockManager.readLock());
  }
}
