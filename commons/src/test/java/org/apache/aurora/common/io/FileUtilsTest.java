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
package org.apache.aurora.common.io;

import java.io.File;
import java.io.IOException;

import com.google.common.io.Files;

import org.apache.aurora.common.base.ExceptionalClosure;
import org.apache.aurora.common.base.ExceptionalFunction;
import org.apache.aurora.common.base.Function;
import org.apache.aurora.common.testing.TearDownTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author John Sirois
 */
public class FileUtilsTest extends TearDownTestCase {

  private FileUtils.Temporary temporary;

  @Before
  public void setUp() {
    final File tmpDir = FileUtils.createTempDir();
    addTearDown(new TearDown() {
      @Override public void tearDown() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(tmpDir);
      }
    });
    assertEmptyDir(tmpDir);

    temporary = new FileUtils.Temporary(tmpDir);
  }

  @Test
  public void testCreateDir() {
    File tmpDir = temporary.createDir();
    assertEmptyDir(tmpDir);
  }

  @Test
  public void testCreateFile() throws IOException {
    File tmpFile = temporary.createFile(".jake");
    assertEmptyFile(tmpFile);
    assertTrue(tmpFile.getName().matches(".+\\.jake$"));
  }

  @Test
  public void testDoWithDir() {
    assertEquals("42", temporary.doWithDir(new Function<File, String>() {
      @Override public String apply(File dir) {
        assertEmptyDir(dir);
        return "42";
      }
    }));
  }

  static class MarkerException extends Exception {}

  @Test(expected = MarkerException.class)
  public void testDoWithDir_bubbles() throws MarkerException {
    temporary.doWithDir(new ExceptionalClosure<File, MarkerException>() {
      @Override public void execute (File dir) throws MarkerException {
        throw new MarkerException();
      }
    });
  }

  @Test
  public void testDoWithFile() throws IOException {
    assertEquals("37", temporary.doWithFile(new ExceptionalFunction<File, String, IOException>() {
      @Override public String apply(File file) throws IOException {
        assertEmptyFile(file);
        return "37";
      }
    }));
  }

  @Test(expected = MarkerException.class)
  public void testDoWithFile_bubbles() throws MarkerException, IOException {
    temporary.doWithFile(new ExceptionalClosure<File, MarkerException>() {
      @Override public void execute(File dir) throws MarkerException {
        throw new MarkerException();
      }
    });
  }

  private void assertEmptyDir(File dir) {
    assertNotNull(dir);
    assertTrue(dir.exists());
    assertTrue(dir.isDirectory());
    assertEquals(0, dir.list().length);
  }

  private void assertEmptyFile(File file) throws IOException {
    assertNotNull(file);
    assertTrue(file.exists());
    assertTrue(file.isFile());
    assertEquals(0, Files.toByteArray(file).length);
  }
}
