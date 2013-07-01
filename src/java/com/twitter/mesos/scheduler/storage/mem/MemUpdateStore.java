package com.twitter.mesos.scheduler.storage.mem;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.JobKey;
import com.twitter.mesos.gen.JobUpdateConfiguration;
import com.twitter.mesos.scheduler.storage.UpdateStore;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory update store.
 */
class MemUpdateStore implements UpdateStore.Mutable {

  private static final Function<JobUpdateConfiguration, JobUpdateConfiguration> DEEP_COPY =
      Util.deepCopier();

  private final Map<String, JobUpdateConfiguration> configs = Maps.newConcurrentMap();

  private String key(String role, String job) {
    checkNotNull(role);
    checkNotNull(job);

    return Tasks.jobKey(role, job);
  }

  private String key(JobUpdateConfiguration config) {
    checkNotNull(config);

    return key(config.getRole(),  config.getJob());
  }

  @Override
  public void saveJobUpdateConfig(JobUpdateConfiguration config) {
    configs.put(key(config), DEEP_COPY.apply(config));
  }

  @Override
  public void removeShardUpdateConfigs(String role, String job) {
    configs.remove(key(role, job));
  }

  @Override
  public void removeShardUpdateConfigs(JobKey jobKey) {
    // TODO(ksweeney): Remove this delegation as part of MESOS-2403.
    removeShardUpdateConfigs(jobKey.getRole(), jobKey.getName());
  }

  @Override
  public void deleteShardUpdateConfigs() {
    configs.clear();
  }

  @Override
  public Optional<JobUpdateConfiguration> fetchJobUpdateConfig(JobKey jobKey) {
    // TODO(ksweeney): Stop ignoring environment here as part of MESOS-2403.
    return Optional.fromNullable(
        configs.get(key(jobKey.getRole(), jobKey.getName()))).transform(DEEP_COPY);
  }

  @Override
  public Set<JobUpdateConfiguration> fetchUpdateConfigs(String role) {
    return FluentIterable.from(configs.values())
        .filter(hasRole(role))
        .transform(DEEP_COPY)
        .toSet();
  }

  @Override
  public Set<String> fetchUpdatingRoles() {
    return FluentIterable.from(configs.values())
        .transform(GET_ROLE)
        .toSet();
  }

  private static final Function<JobUpdateConfiguration, String> GET_ROLE =
      new Function<JobUpdateConfiguration, String>() {
        @Override public String apply(JobUpdateConfiguration config) {
          return config.getRole();
        }
      };

  private static Predicate<JobUpdateConfiguration> hasRole(String role) {
    checkNotNull(role);

    return Predicates.compose(Predicates.equalTo(role), GET_ROLE);
  }
}
