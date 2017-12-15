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
package org.apache.aurora.scheduler.storage.durability;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.storage.durability.Persistence.Edit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RecoveryTest {

  @Test
  public void testRecover() {
    ListPersistence from = new ListPersistence(
        Edit.op(Op.saveQuota(new SaveQuota())),
        Edit.op(Op.saveTasks(new SaveTasks())));
    ListPersistence to = new ListPersistence();

    Recovery.copy(from, to, 100);

    assertEquals(from.edits, to.edits);
  }

  @Test
  public void testRecoverWithDeleteAll() {
    ListPersistence from = new ListPersistence(
        Edit.deleteAll(),
        Edit.op(Op.saveQuota(new SaveQuota())),
        Edit.op(Op.saveTasks(new SaveTasks())));
    ListPersistence to = new ListPersistence();

    Recovery.copy(from, to, 100);

    assertEquals(from.edits.subList(1, from.edits.size()), to.edits);
  }

  @Test
  public void testRequiresEmptyTarget() {
    ListPersistence from = new ListPersistence();
    ListPersistence to = new ListPersistence(Edit.op(Op.saveQuota(new SaveQuota())));

    try {
      Recovery.copy(from, to, 100);
      fail();
    } catch (IllegalStateException e) {
      // expected.
    }
  }

  @Test
  public void testDeleteAllAfterFirstPosition() {
    ListPersistence from = new ListPersistence(
        Edit.op(Op.saveQuota(new SaveQuota())),
        Edit.deleteAll(),
        Edit.op(Op.saveTasks(new SaveTasks())));
    ListPersistence to = new ListPersistence();

    try {
      Recovery.copy(from, to, 100);
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  private static class ListPersistence implements Persistence {

    private final List<Edit> edits;

    ListPersistence(Edit... edits) {
      this.edits = Lists.newArrayList(edits);
    }

    @Override
    public void prepare() {
      // no-op
    }

    @Override
    public Stream<Edit> recover() {
      return edits.stream();
    }

    @Override
    public void persist(Stream<Op> records) throws PersistenceException {
      edits.addAll(records.map(Edit::op).collect(Collectors.toList()));
    }
  }
}
