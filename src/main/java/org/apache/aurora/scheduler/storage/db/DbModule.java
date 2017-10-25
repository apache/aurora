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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;

import org.apache.aurora.common.inject.Bindings.KeyFactory;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.async.GatedWorkQueue;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.db.typehandlers.TypeHandlers;
import org.apache.ibatis.migration.JavaMigrationLoader;
import org.apache.ibatis.migration.MigrationLoader;
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

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-enable_db_metrics",
        description =
            "Whether to use MyBatis interceptor to measure the timing of intercepted Statements.",
        arity = 1)
    public boolean enableDbMetrics = true;

    @Parameter(names = "-slow_query_log_threshold",
        description = "Log all queries that take at least this long to execute.")
    public TimeAmount slowQueryLogThreshold = new TimeAmount(25, Time.MILLISECONDS);

    @Parameter(names = "-db_row_gc_interval",
        description = "Interval on which to scan the database for unused row references.")
    public TimeAmount dbRowGcInterval = new TimeAmount(2, Time.HOURS);

    // http://h2database.com/html/grammar.html#set_lock_timeout
    @Parameter(names = "-db_lock_timeout", description = "H2 table lock timeout")
    public TimeAmount h2LockTimeout = new TimeAmount(1, Time.MINUTES);

    @Parameter(names = "-db_max_active_connection_count",
        description = "Max number of connections to use with database via MyBatis")
    public int mybatisMaxActiveConnectionCount = -1;

    @Parameter(names = "-db_max_idle_connection_count",
        description = "Max number of idle connections to the database via MyBatis")
    public int mybatisMaxIdleConnectionCount = -1;
  }

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
      .add(MigrationMapper.class)
      .add(QuotaMapper.class)
      .add(TaskConfigMapper.class)
      .add(TaskMapper.class)
      .build();

  private final Options options;
  private final KeyFactory keyFactory;
  private final String jdbcSchema;

  private DbModule(
      Options options,
      KeyFactory keyFactory,
      String dbName,
      Map<String, String> jdbcUriArgs) {

    this.options = requireNonNull(options);
    this.keyFactory = requireNonNull(keyFactory);

    Map<String, String> args = ImmutableMap.<String, String>builder()
        .putAll(jdbcUriArgs)
        // READ COMMITTED transaction isolation.  More details here
        // http://www.h2database.com/html/advanced.html?#transaction_isolation
        .put("LOCK_MODE", "3")
        // Send log messages from H2 to SLF4j
        // See http://www.h2database.com/html/features.html#other_logging
        .put("TRACE_LEVEL_FILE", "4")
        // Enable Query Statistics
        .put("QUERY_STATISTICS", "TRUE")
        // Configure the lock timeout
        .put("LOCK_TIMEOUT", options.h2LockTimeout.as(Time.MILLISECONDS).toString())
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
  public static Module productionModule(KeyFactory keyFactory, DbModule.Options options) {
    return new DbModule(
        options,
        keyFactory,
        "aurora",
        ImmutableMap.of("DB_CLOSE_DELAY", "-1"));
  }

  @VisibleForTesting
  public static Module testModule(KeyFactory keyFactory) {
    DbModule.Options options = new DbModule.Options();
    return new DbModule(
        options,
        keyFactory,
        "testdb-" + UUID.randomUUID().toString(),
        // A non-zero close delay is used here to avoid eager database cleanup in tests that
        // make use of multiple threads.  Since all test databases are separately scoped by the
        // included UUID, multiple DB instances will overlap in time but they should be distinct
        // in content.
        ImmutableMap.of("DB_CLOSE_DELAY", "5"));
  }

  /**
   * Same as {@link #testModuleWithWorkQueue(KeyFactory)} but with default task store and
   * key factory.
   *
   * @return A new database module for testing.
   */
  @VisibleForTesting
  public static Module testModule() {
    return testModule(KeyFactory.PLAIN);
  }

  /**
   * Creates a module that will prepare a private in-memory database, using a specific task store
   * implementation bound within the key factory and provided module.
   *
   * @param keyFactory Key factory to use.
   * @return A new database module for testing.
   */
  @VisibleForTesting
  public static Module testModuleWithWorkQueue(KeyFactory keyFactory) {
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
        testModule(keyFactory)
    );
  }

  /**
   * Same as {@link #testModuleWithWorkQueue(KeyFactory)} but with default key factory.
   *
   * @return A new database module for testing.
   */
  @VisibleForTesting
  public static Module testModuleWithWorkQueue() {
    return testModuleWithWorkQueue(KeyFactory.PLAIN);
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
        if (options.enableDbMetrics) {
          addInterceptorClass(InstrumentingInterceptor.class);
        }

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

        bind(new TypeLiteral<Amount<Long, Time>>() { })
            .toInstance(options.slowQueryLogThreshold);

        // Enable a ping query which will prevent the use of invalid connections in the
        // connection pool.
        bindProperties(binder(), ImmutableMap.of("mybatis.pooled.pingEnabled", "true"));
        bindProperties(binder(), ImmutableMap.of("mybatis.pooled.pingQuery", "SELECT 1;"));

        if (options.mybatisMaxActiveConnectionCount > 0) {
          String val = String.valueOf(options.mybatisMaxActiveConnectionCount);
          bindProperties(binder(), ImmutableMap.of("mybatis.pooled.maximumActiveConnections", val));
        }

        if (options.mybatisMaxIdleConnectionCount > 0) {
          String val = String.valueOf(options.mybatisMaxIdleConnectionCount);
          bindProperties(binder(), ImmutableMap.of("mybatis.pooled.maximumIdleConnections", val));
        }

        // Exposed for unit tests.
        bind(TaskConfigManager.class);
        expose(TaskConfigManager.class);

        // TODO(wfarner): Don't expose these bindings once the task store is directly bound here.
        expose(TaskMapper.class);
        expose(TaskConfigManager.class);
        expose(JobKeyMapper.class);
      }
    });
    expose(keyFactory.create(CronJobStore.Mutable.class));
    expose(keyFactory.create(TaskStore.Mutable.class));

    bindStore(AttributeStore.Mutable.class, DbAttributeStore.class);
    bindStore(LockStore.Mutable.class, DbLockStore.class);
    bindStore(QuotaStore.Mutable.class, DbQuotaStore.class);
    bindStore(SchedulerStore.Mutable.class, DbSchedulerStore.class);
    bindStore(JobUpdateStore.Mutable.class, DbJobUpdateStore.class);
    bindStore(TaskStore.Mutable.class, DbTaskStore.class);
    bindStore(CronJobStore.Mutable.class, DbCronJobStore.class);

    Key<Storage> storageKey = keyFactory.create(Storage.class);
    bind(storageKey).to(DbStorage.class);
    bind(DbStorage.class).in(Singleton.class);
    expose(storageKey);

    bind(EnumBackfill.class).to(EnumBackfill.EnumBackfillImpl.class);
    bind(EnumBackfill.EnumBackfillImpl.class).in(Singleton.class);
    expose(EnumBackfill.class);

    expose(DbStorage.class);
    expose(SqlSessionFactory.class);
    expose(TaskMapper.class);
    expose(TaskConfigMapper.class);
    expose(JobKeyMapper.class);
  }

  /**
   * Module that sets up a periodic database garbage-collection routine.
   */
  public static class GarbageCollectorModule extends AbstractModule {

    private final Options options;

    public GarbageCollectorModule(Options options) {
      this.options = options;
    }

    @Override
    protected void configure() {
      install(new PrivateModule() {
        @Override
        protected void configure() {
          bind(RowGarbageCollector.class).in(Singleton.class);
          bind(AbstractScheduledService.Scheduler.class).toInstance(
              AbstractScheduledService.Scheduler.newFixedRateSchedule(
                  0L,
                  options.dbRowGcInterval.getValue(),
                  options.dbRowGcInterval.getUnit().getTimeUnit()));
          expose(RowGarbageCollector.class);
        }
      });
      SchedulerServicesModule.addSchedulerActiveServiceBinding(binder())
          .to(RowGarbageCollector.class);
    }
  }

  public static class MigrationManagerModule extends PrivateModule {
    private static final String MIGRATION_PACKAGE =
        "org.apache.aurora.scheduler.storage.db.migration";

    private final MigrationLoader migrationLoader;

    public MigrationManagerModule() {
      this.migrationLoader = new JavaMigrationLoader(MIGRATION_PACKAGE);
    }

    public MigrationManagerModule(MigrationLoader migrationLoader) {
      this.migrationLoader = requireNonNull(migrationLoader);
    }

    @Override
    protected void configure() {
      bind(MigrationLoader.class).toInstance(migrationLoader);

      bind(MigrationManager.class).to(MigrationManagerImpl.class);
      expose(MigrationManager.class);
    }
  }
}
