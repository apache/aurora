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

import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.twitter.common.inject.Bindings;

import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.db.typehandlers.TypeHandlers;
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
 * <p>
 *   Currently only exposes bindings for:
 *   <ul>
 *     <li>{@link org.apache.aurora.scheduler.storage.db.DbStorage}</li>
 *     <li>{@link org.apache.ibatis.session.SqlSessionFactory}</li>
 *     <li>Keys provided by the provided{@code keyFactory} for:
 *        <ul>
 *          <li>{@link LockStore.Mutable}</li>
 *          <li>{@link QuotaStore.Mutable}</li>
 *          <li>{@link SchedulerStore.Mutable}</li>
 *        </ul>
 *     </li>
 *   </ul>
 * </p>
 */
public class DbModule extends PrivateModule {

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
      .build();

  private final Bindings.KeyFactory keyFactory;
  private final String jdbcSchema;

  private DbModule(Bindings.KeyFactory keyFactory, String jdbcSchema) {
    this.keyFactory = requireNonNull(keyFactory);
    this.jdbcSchema = requireNonNull(jdbcSchema);
  }

  public DbModule(Bindings.KeyFactory keyFactory) {
    this(keyFactory, "aurora;DB_CLOSE_DELAY=-1");
  }

  /**
   * Creates a module that will prepare a private in-memory database.
   *
   * @param keyFactory Key factory to scope bindings.
   * @return A new database module for testing.
   */
  @VisibleForTesting
  public static DbModule testModule(Bindings.KeyFactory keyFactory) {
    // This creates a private in-memory database.  New connections will have a _new_ database,
    // and closing the database will expunge its data.
    return new DbModule(keyFactory, "");
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
      }
    });
    bindStore(AttributeStore.Mutable.class, DbAttributeStore.class);
    bindStore(LockStore.Mutable.class, DbLockStore.class);
    bindStore(QuotaStore.Mutable.class, DbQuotaStore.class);
    bindStore(SchedulerStore.Mutable.class, DbSchedulerStore.class);
    bindStore(JobUpdateStore.Mutable.class, DBJobUpdateStore.class);

    Key<Storage> storageKey = keyFactory.create(Storage.class);
    bind(storageKey).to(DbStorage.class);
    bind(DbStorage.class).in(Singleton.class);
    expose(storageKey);

    expose(DbStorage.class);
    expose(SqlSessionFactory.class);
  }
}
