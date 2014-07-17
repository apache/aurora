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
import java.io.InputStreamReader;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.JobStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.ibatis.builder.StaticSqlSource;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.logging.jdk14.Jdk14LoggingImpl;
import org.apache.ibatis.mapping.MappedStatement.Builder;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.guice.transactional.Transactional;

import static java.util.Objects.requireNonNull;

/**
 * A storage implementation backed by a relational database.
 *
 * <p>Delegates read and write concurrency semantics to the underlying database.
 * In this implementation, {@link #weaklyConsistentRead(Work)} and {@link #consistentRead(Work)}
 * have identical behaviour as they are both annotated by
 * {@link org.mybatis.guice.transactional.Transactional}. This class is currently only
 * partially implemented, with the underlying {@link MutableStoreProvider} only providing
 * a {@link LockStore.Mutable} implementation. It is designed to be a long term replacement
 * for {@link org.apache.aurora.scheduler.storage.mem.MemStorage}.</p>
 */
class DbStorage extends AbstractIdleService implements Storage {

  private final SqlSessionFactory sessionFactory;
  private final MutableStoreProvider storeProvider;
  private final EnumValueMapper enumValueMapper;

  @Inject
  DbStorage(
      SqlSessionFactory sessionFactory,
      EnumValueMapper enumValueMapper,
      final AttributeStore.Mutable attributeStore,
      final LockStore.Mutable lockStore,
      final QuotaStore.Mutable quotaStore) {

    this.sessionFactory = requireNonNull(sessionFactory);
    this.enumValueMapper = requireNonNull(enumValueMapper);
    requireNonNull(attributeStore);
    requireNonNull(lockStore);
    requireNonNull(quotaStore);
    storeProvider = new MutableStoreProvider() {
      @Override
      public SchedulerStore.Mutable getSchedulerStore() {
        throw new UnsupportedOperationException("Not yet implemented.");
      }

      @Override
      public JobStore.Mutable getJobStore() {
        throw new UnsupportedOperationException("Not yet implemented.");
      }

      @Override
      public TaskStore getTaskStore() {
        throw new UnsupportedOperationException("Not yet implemented.");
      }

      @Override
      public TaskStore.Mutable getUnsafeTaskStore() {
        throw new UnsupportedOperationException("Not yet implemented.");
      }

      @Override
      public LockStore.Mutable getLockStore() {
        return lockStore;
      }

      @Override
      public QuotaStore.Mutable getQuotaStore() {
        return quotaStore;
      }

      @Override
      public AttributeStore.Mutable getAttributeStore() {
        return attributeStore;
      }
    };
  }

  @Override
  @Transactional
  public <T, E extends Exception> T consistentRead(Work<T, E> work) throws StorageException, E {
    try {
      return work.apply(storeProvider);
    } catch (PersistenceException e) {
      throw new StorageException(e.getMessage(), e);
    }
  }

  @Override
  @Transactional
  public <T, E extends Exception> T weaklyConsistentRead(Work<T, E> work)
      throws StorageException, E {

    try {
      return work.apply(storeProvider);
    } catch (PersistenceException e) {
      throw new StorageException(e.getMessage(), e);
    }
  }

  @Override
  @Transactional
  public <T, E extends Exception> T write(MutateWork<T, E> work) throws StorageException, E {
    try {
      return work.apply(storeProvider);
    } catch (PersistenceException e) {
      throw new StorageException(e.getMessage(), e);
    }
  }

  @Override
  public void prepare() {
    startAsync().awaitRunning();
  }

  /**
   * Creates the SQL schema during service start-up.
   * Note: This design assumes a volatile database engine.
   */
  @Override
  @Transactional
  protected void startUp() throws IOException {
    Configuration configuration = sessionFactory.getConfiguration();
    String createStatementName = "create_tables";
    configuration.setMapUnderscoreToCamelCase(true);
    configuration.addMappedStatement(new Builder(
        configuration,
        createStatementName,
        new StaticSqlSource(
            configuration,
            CharStreams.toString(
                new InputStreamReader(
                    DbStorage.class.getResourceAsStream("schema.sql"),
                    Charsets.UTF_8))),
        SqlCommandType.UPDATE)
        .build());

    configuration.setLogImpl(Jdk14LoggingImpl.class);

    try (SqlSession session = sessionFactory.openSession()) {
      session.update(createStatementName);
    }

    for (MaintenanceMode mode : MaintenanceMode.values()) {
      enumValueMapper.addEnumValue("maintenance_modes", mode.getValue(), mode.name());
    }
  }

  @Override
  protected void shutDown() {
    // noop
  }
}
