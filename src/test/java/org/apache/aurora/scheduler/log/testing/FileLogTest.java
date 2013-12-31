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
package com.twitter.aurora.scheduler.log.testing;

import java.io.File;
import java.io.IOException;

import com.google.common.io.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;

public class FileLogTest {

  private File tempDir;
  private File testingLogFile;

  @Before
  public void setUp() {
    tempDir = Files.createTempDir();
    testingLogFile = new File(tempDir.getAbsolutePath(), "/log_file");
  }

  @After
  public void tearDown() {
    testingLogFile.delete();
    tempDir.delete();
  }

  @Test
  public void testNewFile() throws IOException {
    FileLog log = new FileLog(testingLogFile);
    assertNotNull(log.open());
  }

  @Test
  public void testEmptyFile() throws IOException {
    testingLogFile.createNewFile();
    FileLog log = new FileLog(testingLogFile);
    assertNotNull(log.open());
  }
}
