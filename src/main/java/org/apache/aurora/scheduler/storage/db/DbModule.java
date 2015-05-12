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

import java.util.Set;
import java.util.UUID;

import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.inject.Bindings.KeyFactory;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.db.typehandlers.TypeHandlers;
import org.apache.aurora.scheduler.storage.mem.InMemStoresModule;
import org.apache.ibatis.session.AutoMappingBehavior;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.mybatis.guice.MyBatisModule;
import org.mybatis.guice.datasource.builtin.PooledDataSourceProvider;
import org.mybatis.guice.datasource.helper.JdbcHelper;

import static java.util.Objects.requireNonNull;

import static com.google.inject.name.Names.bindProperties;

/**
 * Binding module for a relational database storage system.
 */
public final class DbModule extends PrivateModule {

  @CmdLine(name = "use_beta_db_task_store",
      help = "Whether to use the experimental database-backed task store.")
  private static final Arg<Boolean> USE_DB_TASK_STORE = Arg.create(false);

  @CmdLine(name = "slow_query_log_threshold",
      help = "Log all queries that take at least this long to execute.")
  private static final Arg<Amount<Long, Time>> SLOW_QUERY_LOG_THRESHOLD =
      Arg.create(Amount.of(25L, Time.MILLISECONDS));

  private static final Set<Class<?>> MAPPER_CLASSES = ImmutableSet.<Class<?>>builder()
      .add(AttributeMapper.class)
      .add(EnumValueMapper.class)
      .add(FrameworkIdMapper.class)
      .add(JobInstanceUpdateEventMapper.class)
      .add(JobKeyMapper.class)
      .add(JobUpdateEventMapper.class)
      .add(JobUpdateDetailsMapper.class)
      .add(LockMapper.class)
      .add(QuotaMapper.class)
      .add(TaskConfigMapper.class)
      .add(TaskMapper.class)
      .build();

  private final KeyFactory keyFactory;
  private final Module taskStoreModule;
  private final String jdbcSchema;

  private DbModule(KeyFactory keyFactory, Module taskStoreModule, String jdbcSchema) {
    this.keyFactory = requireNonNull(keyFactory);
    this.taskStoreModule = requireNonNull(taskStoreModule);
    // We always disable the MvStore, as it is in beta as of this writing.
    this.jdbcSchema = jdbcSchema + ";MV_STORE=false";
  }

  /**
   * Creates a module that will prepare a volatile storage system suitable for use in a production
   * environment.
   *
   * @param keyFactory Binding scope for the storage system.
   * @return A new database module for production.
   */
  public static Module productionModule(KeyFactory keyFactory) {
    return new DbModule(
        keyFactory,
        USE_DB_TASK_STORE.get()
            ? new TaskStoreModule(keyFactory)
            : new InMemStoresModule.TaskStoreModule(keyFactory),
        "aurora;DB_CLOSE_DELAY=-1");
  }

  /**
   * Creates a module that will prepare a private in-memory database, using a specific task store
   * implementation bound within the provided module.
   *
   * @param taskStoreModule Module providing task store bindings.
   * @return A new database module for testing.
   */
  @VisibleForTesting
  public static Module testModule(Module taskStoreModule) {
    return new DbModule(
        KeyFactory.PLAIN,
        taskStoreModule,
        // A non-zero close delay is used here to avoid eager database cleanup in tests that
        // make use of multiple threads.  Since all test databases are separately scoped by the
        // included UUID, multiple DB instances will overlap in time but they should be distinct
        // in content.
        "testdb-" + UUID.randomUUID().toString() + ";DB_CLOSE_DELAY=5");
  }

  /**
   * Creates a module that will prepare a private in-memory database.
   *
   * @return A new database module for testing.
   */
  @VisibleForTesting
  public static Module testModule() {
    return testModule(new DbModule.TaskStoreModule(KeyFactory.PLAIN));
  }

  private <T> void bindStore(Class<T> binding, Class<? extends T> impl) {
    bind(binding).to(impl);
    bind(impl).in(Singleton.class);
    Key<T> key = keyFactory.create(binding);
    bind(key).to(impl);
    expose(key);
  }

  @Override
  protected void configure() {
    install(new MyBatisModule() {
      @Override
      protected void initialize() {
        bindProperties(binder(), ImmutableMap.of("JDBC.schema", jdbcSchema));
        install(JdbcHelper.H2_IN_MEMORY_NAMED);

        // We have no plans to take advantage of multiple DB environments. This is a
        // required property though, so we use an unnamed environment.
        environmentId("");

        bindTransactionFactoryType(JdbcTransactionFactory.class);
        bindDataSourceProviderType(PooledDataSourceProvider.class);
        addMapperClasses(MAPPER_CLASSES);

        // Full auto-mapping enables population of nested objects with minimal mapper configuration.
        // Docs on settings can be found here:
        // http://mybatis.github.io/mybatis-3/configuration.html#settings
        autoMappingBehavior(AutoMappingBehavior.FULL);

        addTypeHandlersClasses(TypeHandlers.getAll());

        bind(new TypeLiteral<Amount<Long, Time>>() { }).toInstance(SLOW_QUERY_LOG_THRESHOLD.get());

        // Exposed for unit tests.
        bind(TaskConfigManager.class);
        expose(TaskConfigManager.class);

        // TODO(wfarner): Don't expose these bindings once the task store is directly bound here.
        expose(TaskMapper.class);
        expose(TaskConfigManager.class);
        expose(JobKeyMapper.class);
      }
    });
    install(new InMemStoresModule(keyFactory));
    install(taskStoreModule);
    expose(keyFactory.create(CronJobStore.Mutable.class));
    expose(keyFactory.create(TaskStore.Mutable.class));

    bindStore(AttributeStore.Mutable.class, DbAttributeStore.class);
    bindStore(LockStore.Mutable.class, DbLockStore.class);
    bindStore(QuotaStore.Mutable.class, DbQuotaStore.class);
    bindStore(SchedulerStore.Mutable.class, DbSchedulerStore.class);
    bindStore(JobUpdateStore.Mutable.class, DbJobUpdateStore.class);

    Key<Storage> storageKey = keyFactory.create(Storage.class);
    bind(storageKey).to(DbStorage.class);
    bind(DbStorage.class).in(Singleton.class);
    expose(storageKey);

    expose(DbStorage.class);
    expose(SqlSessionFactory.class);
  }

  /**
   * A module that binds a database task store.
   * <p/>
   * TODO(wfarner): Inline these bindings once there is only one task store implementation.
   */
  public static class TaskStoreModule extends PrivateModule {
    private final KeyFactory keyFactory;

    public TaskStoreModule(KeyFactory keyFactory) {
      this.keyFactory = requireNonNull(keyFactory);
    }

    private <T> void bindStore(Class<T> binding, Class<? extends T> impl) {
      bind(binding).to(impl);
      bind(impl).in(Singleton.class);
      Key<T> key = keyFactory.create(binding);
      bind(key).to(impl);
      expose(key);
    }

    @Override
    protected void configure() {
      bindStore(TaskStore.Mutable.class, DbTaskStore.class);
      expose(TaskStore.Mutable.class);
    }
  }
}
