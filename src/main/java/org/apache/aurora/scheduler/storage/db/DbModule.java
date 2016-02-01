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

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.inject.Bindings.KeyFactory;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.async.GatedWorkQueue;
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

  @CmdLine(name = "db_row_gc_interval",
      help = "Interval on which to scan the database for unused row references.")
  private static final Arg<Amount<Long, Time>> DB_ROW_GC_INTERVAL =
      Arg.create(Amount.of(2L, Time.HOURS));

  // http://h2database.com/html/grammar.html#set_lock_timeout
  @CmdLine(name = "db_lock_timeout", help = "H2 table lock timeout")
  private static final Arg<Amount<Long, Time>> H2_LOCK_TIMEOUT =
      Arg.create(Amount.of(1L, Time.MINUTES));

  private static final Set<Class<?>> MAPPER_CLASSES = ImmutableSet.<Class<?>>builder()
      .add(AttributeMapper.class)
      .add(CronJobMapper.class)
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
  private final Module taskStoresModule;
  private final String jdbcSchema;

  private DbModule(
      KeyFactory keyFactory,
      Module taskStoresModule,
      String dbName,
      Map<String, String> jdbcUriArgs) {

    this.keyFactory = requireNonNull(keyFactory);
    this.taskStoresModule = requireNonNull(taskStoresModule);

    Map<String, String> args = ImmutableMap.<String, String>builder()
        .putAll(jdbcUriArgs)
        // We always disable the MvStore, as it is in beta as of this writing.
        .put("MV_STORE", "false")
        // READ COMMITTED transaction isolation.  More details here
        // http://www.h2database.com/html/advanced.html?#transaction_isolation
        .put("LOCK_MODE", "3")
        // Send log messages from H2 to SLF4j
        // See http://www.h2database.com/html/features.html#other_logging
        .put("TRACE_LEVEL_FILE", "4")
        // Enable Query Statistics
        .put("QUERY_STATISTICS", "TRUE")
        // Configure the lock timeout
        .put("LOCK_TIMEOUT", H2_LOCK_TIMEOUT.get().as(Time.MILLISECONDS).toString())
        .build();
    this.jdbcSchema = dbName + ";" + Joiner.on(";").withKeyValueSeparator("=").join(args);
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
        getTaskStoreModule(keyFactory),
        "aurora",
        ImmutableMap.of("DB_CLOSE_DELAY", "-1"));
  }

  /**
   * Creates a module that will prepare a private in-memory database, using a specific task store
   * implementation bound within the key factory and provided module.
   *
   * @param keyFactory Key factory to use.
   * @param taskStoreModule Module providing task store bindings.
   * @return A new database module for testing.
   */
  @VisibleForTesting
  public static Module testModule(KeyFactory keyFactory, Optional<Module> taskStoreModule) {
    return Modules.combine(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(GatedWorkQueue.class).annotatedWith(AsyncExecutor.class).toInstance(
                new GatedWorkQueue() {
                  @Override
                  public <T, E extends Exception> T closeDuring(
                      GatedOperation<T, E> operation) throws E {

                    return operation.doWithGateClosed();
                  }
                });
          }
        },
        new DbModule(
            keyFactory,
            taskStoreModule.isPresent() ? taskStoreModule.get() : getTaskStoreModule(keyFactory),
            "testdb-" + UUID.randomUUID().toString(),
            // A non-zero close delay is used here to avoid eager database cleanup in tests that
            // make use of multiple threads.  Since all test databases are separately scoped by the
            // included UUID, multiple DB instances will overlap in time but they should be distinct
            // in content.
            ImmutableMap.of("DB_CLOSE_DELAY", "5"))
    );
  }

  /**
   * Same as {@link #testModule(KeyFactory, Optional)} but with default task store and key factory.
   *
   * @return A new database module for testing.
   */
  @VisibleForTesting
  public static Module testModule() {
    return testModule(KeyFactory.PLAIN, Optional.of(new TaskStoreModule(KeyFactory.PLAIN)));
  }

  private static Module getTaskStoreModule(KeyFactory keyFactory) {
    return USE_DB_TASK_STORE.get()
        ? new TaskStoreModule(keyFactory)
        : new InMemStoresModule(keyFactory);
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

        // Enable a ping query which will prevent the use of invalid connections in the
        // connection pool.
        bindProperties(binder(), ImmutableMap.of("mybatis.pooled.pingEnabled", "true"));
        bindProperties(binder(), ImmutableMap.of("mybatis.pooled.pingQuery", "SELECT 1;"));

        // Exposed for unit tests.
        bind(TaskConfigManager.class);
        expose(TaskConfigManager.class);

        // TODO(wfarner): Don't expose these bindings once the task store is directly bound here.
        expose(TaskMapper.class);
        expose(TaskConfigManager.class);
        expose(JobKeyMapper.class);
      }
    });
    install(taskStoresModule);
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
    expose(TaskMapper.class);
    expose(TaskConfigMapper.class);
    expose(JobKeyMapper.class);
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
      bindStore(CronJobStore.Mutable.class, DbCronJobStore.class);
      expose(DbCronJobStore.Mutable.class);
    }
  }

  /**
   * Module that sets up a periodic database garbage-collection routine.
   */
  public static class GarbageCollectorModule extends AbstractModule {
    @Override
    protected void configure() {
      install(new PrivateModule() {
        @Override
        protected void configure() {
          bind(RowGarbageCollector.class).in(Singleton.class);
          bind(AbstractScheduledService.Scheduler.class).toInstance(
              AbstractScheduledService.Scheduler.newFixedRateSchedule(
                  0L,
                  DB_ROW_GC_INTERVAL.get().getValue(),
                  DB_ROW_GC_INTERVAL.get().getUnit().getTimeUnit()));
          expose(RowGarbageCollector.class);
        }
      });
      SchedulerServicesModule.addSchedulerActiveServiceBinding(binder())
          .to(RowGarbageCollector.class);
    }
  }
}
