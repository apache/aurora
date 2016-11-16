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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import javax.inject.Inject;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

import org.apache.aurora.scheduler.storage.db.views.MigrationChangelogEntry;
import org.apache.ibatis.migration.Change;
import org.apache.ibatis.migration.DataSourceConnectionProvider;
import org.apache.ibatis.migration.MigrationLoader;
import org.apache.ibatis.migration.operations.UpOperation;
import org.apache.ibatis.migration.options.DatabaseOperationOption;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class MigrationManagerImpl implements MigrationManager {
  private static final Logger LOG = LoggerFactory.getLogger(MigrationManagerImpl.class);

  private final SqlSessionFactory sqlSessionFactory;
  private final MigrationLoader migrationLoader;

  @Inject
  MigrationManagerImpl(SqlSessionFactory sqlSessionFactory, MigrationLoader migrationLoader) {
    this.sqlSessionFactory = requireNonNull(sqlSessionFactory);
    this.migrationLoader = requireNonNull(migrationLoader);
  }

  @Override
  public void migrate() throws SQLException {
    LOG.info("Running db migrations.");

    try (SqlSession sqlSession = sqlSessionFactory.openSession(true /* auto commit */)) {
      MigrationMapper mapper = sqlSession.getMapper(MigrationMapper.class);

      LOG.info("Bootstrapping changelog table (if necessary).");
      mapper.bootstrapChangelog();

      if (!checkRollback(mapper, migrationLoader.getMigrations())) {
        DatabaseOperationOption options = new DatabaseOperationOption();
        options.setAutoCommit(true);

        new UpOperation().operate(
            new DataSourceConnectionProvider(
                sqlSessionFactory.getConfiguration().getEnvironment().getDataSource()),
            migrationLoader,
            options,
            null);

        saveDowngradeScript(mapper);
      }
    }
  }

  /**
   * Iterates applied changes to ensure they all exist on the classpath. For any changes that do not
   * exist on the classpath, their downgrade script is run.
   *
   * @param mapper A {@link MigrationMapper} instance used to modify the changelog.
   * @param changes The list of {@link Change}s found on the classpath.
   * @return true if a rollback was detected, false otherwise.
   * @throws SQLException in the event a SQL failure.
   */
  private boolean checkRollback(MigrationMapper mapper, List<Change> changes) throws SQLException {
    boolean rollback = false;

    List<MigrationChangelogEntry> appliedChanges = mapper.selectAll();

    for (MigrationChangelogEntry change : appliedChanges) {
      // We cannot directly call changes.contains(...) since contains relies on Change#equals
      // which includes class in its equality check rather than checking instanceof. Instead we just
      // find the first element in changes whose id matches our applied change. If it does not exist
      // then this must be a rollback.
      if (changes.stream().anyMatch(c -> c.getId().equals(change.getId()))) {
        LOG.info("Change " + change.getId() + " has been applied, no other downgrades are "
            + "necessary");
        break;
      }

      LOG.info("No migration corresponding to change id " + change.getId() + " found. Assuming "
          + "this is a rollback.");
      LOG.info("Downgrade SQL for " + change.getId() + " is: " + change.getDowngradeScript());

      try (SqlSession session = sqlSessionFactory.openSession(true)) {
        try (Connection c = session.getConnection()) {
          try (PreparedStatement downgrade = c.prepareStatement(change.getDowngradeScript())) {
            downgrade.execute();
            rollback = true;
          }
        }
      }

      LOG.info("Deleting applied change: " + change.getId());
      mapper.delete(change.getId());
    }

    return rollback;
  }

  private void saveDowngradeScript(MigrationMapper mapper) {
    for (Change c : migrationLoader.getMigrations()) {
      try {
        String downgradeScript = CharStreams.toString(migrationLoader.getScriptReader(c, true));
        LOG.info("Saving downgrade script for change id " + c.getId() + ": " + downgradeScript);

        mapper.saveDowngradeScript(c.getId(), downgradeScript.getBytes(Charsets.UTF_8));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
