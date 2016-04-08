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
package org.apache.aurora.scheduler.storage.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.inject.Injector;

import org.apache.aurora.scheduler.storage.db.views.MigrationChangelogEntry;
import org.apache.ibatis.migration.Change;
import org.apache.ibatis.migration.JavaMigrationLoader;
import org.apache.ibatis.migration.MigrationLoader;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.Test;

import static org.apache.aurora.scheduler.storage.db.DbModule.testModuleWithWorkQueue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MigrationManagerImplIT {
  private Injector createMigrationInjector(MigrationLoader migrationLoader) {
    return DbUtil.createStorageInjector(
        testModuleWithWorkQueue(),
        new DbModule.MigrationManagerModule(migrationLoader));
  }

  /**
   * Ensure all changes have been applied and their downgrade scripts stored appropriately.
   *
   * @param sqlSessionFactory The sql session factory.
   * @param loader A migration loader.
   * @throws Exception If the changes were not applied properly.
   */
  private void assertMigrationComplete(
      SqlSessionFactory sqlSessionFactory,
      MigrationLoader loader) throws Exception {

    try (SqlSession session = sqlSessionFactory.openSession()) {
      MigrationMapper mapper = session.getMapper(MigrationMapper.class);

      List<MigrationChangelogEntry> appliedChanges = mapper.selectAll();

      for (Change change : loader.getMigrations()) {
        Optional<MigrationChangelogEntry> appliedChange = appliedChanges
            .stream()
            .filter(c -> c.getId().equals(change.getId()))
            .findFirst();

        assertTrue(appliedChange.isPresent());
        assertEquals(
            CharStreams.toString(loader.getScriptReader(change, true /* undo */)),
            appliedChange.get().getDowngradeScript());
      }

      // As long as the tables exist, neither of these statements should fail.
      try (Connection c = session.getConnection()) {
        try (PreparedStatement select = c.prepareStatement("SELECT * FROM V001_test_table")) {
          select.execute();
        }
        try (PreparedStatement select = c.prepareStatement("SELECT * FROM V002_test_table2")) {
          select.execute();
        }
      }
    }
  }

  @Test
  public void testMigrate() throws Exception {
    MigrationLoader loader = new JavaMigrationLoader(
        "org.apache.aurora.scheduler.storage.db.testmigration");
    Injector injector = createMigrationInjector(loader);

    injector.getInstance(MigrationManager.class).migrate();
    assertMigrationComplete(injector.getInstance(SqlSessionFactory.class), loader);
  }

  @Test
  public void testNoMigrationNecessary() throws Exception {
    MigrationLoader loader = new JavaMigrationLoader(
        "org.apache.aurora.scheduler.storage.db.testmigration");
    Injector injector = createMigrationInjector(loader);

    MigrationManager migrationManager = injector.getInstance(MigrationManager.class);

    migrationManager.migrate();

    SqlSessionFactory sqlSessionFactory = injector.getInstance(SqlSessionFactory.class);
    assertMigrationComplete(sqlSessionFactory, loader);

    // Run the migration a second time, no changes should be made.
    migrationManager.migrate();
    assertMigrationComplete(sqlSessionFactory, loader);
  }

  private void assertRollbackComplete(SqlSessionFactory sqlSessionFactory) throws Exception {
    try (SqlSession session = sqlSessionFactory.openSession()) {
      MigrationMapper mapper = session.getMapper(MigrationMapper.class);

      assertTrue(mapper.selectAll().isEmpty());
      try (Connection c = session.getConnection()) {
        for (String table : ImmutableList.of("V001_test_table", "V002_test_table2")) {
          try (PreparedStatement select = c.prepareStatement("SELECT * FROM " + table)) {
            select.execute();
            fail("Select from " + table + " should have failed, the table should have been "
                + "dropped.");
          } catch (SQLException e) {
            // This exception is expected.
            assertTrue(
                e.getMessage().startsWith("Table \"" + table.toUpperCase() + "\" not found"));
          }
        }
      }
    }
  }

  @Test
  public void testRollback() throws Exception {
    // Run a normal migration which will apply one the change found in the testmigration package.
    MigrationLoader loader = new JavaMigrationLoader(
        "org.apache.aurora.scheduler.storage.db.testmigration");
    Injector injector = createMigrationInjector(loader);

    MigrationManager migrationManager = injector.getInstance(MigrationManager.class);

    migrationManager.migrate();

    // Now we intentionally pass a reference to a non-existent package to ensure that no migrations
    // are found. As such a rollback is expected to be detected and a downgrade be performed.
    MigrationLoader rollbackLoader = new JavaMigrationLoader(
        "org.apache.aurora.scheduler.storage.db.nomigrations");
    Injector rollbackInjector = createMigrationInjector(rollbackLoader);
    MigrationManager rollbackManager = rollbackInjector.getInstance(MigrationManager.class);

    rollbackManager.migrate();

    assertRollbackComplete(rollbackInjector.getInstance(SqlSessionFactory.class));
  }
}
