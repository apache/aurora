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
package org.apache.aurora.common.args;

import java.io.IOException;
import java.lang.reflect.Field;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.common.args.apt.Configuration;
import org.apache.aurora.common.args.apt.Configuration.ArgInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.aurora.common.args.apt.Configuration.ConfigurationException;

/**
 * Utility that can load static {@literal @CmdLine} and {@literal @Positional} arg field info from
 * a configuration database or from explicitly listed containing classes or objects.
 */
public final class Args {
  private static final Logger LOG = LoggerFactory.getLogger(Args.class);

  @VisibleForTesting
  static final Function<ArgInfo, Optional<Field>> TO_FIELD =
      info -> {
        try {
          return Optional.of(Class.forName(info.className).getDeclaredField(info.fieldName));
        } catch (NoSuchFieldException e) {
          throw new ConfigurationException(e);
        } catch (ClassNotFoundException e) {
          throw new ConfigurationException(e);
        } catch (NoClassDefFoundError e) {
          // A compilation had this class available at the time the ArgInfo was deposited, but
          // the classes have been re-bundled with some subset including the class this ArgInfo
          // points to no longer available.  If the re-bundling is correct, then the arg truly is
          // not needed.
          LOG.debug("Not on current classpath, skipping {}", info);
          return Optional.absent();
        }
      };

  private static final Function<Field, OptionInfo<?>> TO_OPTION_INFO =
      field -> {
        @Nullable CmdLine cmdLine = field.getAnnotation(CmdLine.class);
        if (cmdLine == null) {
          throw new ConfigurationException("No @CmdLine Arg annotation for field " + field);
        }
        return OptionInfo.createFromField(field);
      };

  /**
   * An opaque container for all the positional and optional {@link Arg} metadata in-play for a
   * command line parse.
   */
  public static final class ArgsInfo {
    private final Configuration configuration;
    private final ImmutableList<? extends OptionInfo<?>> optionInfos;

    ArgsInfo(Configuration configuration, Iterable<? extends OptionInfo<?>> optionInfos) {
      this.configuration = Preconditions.checkNotNull(configuration);
      this.optionInfos = ImmutableList.copyOf(optionInfos);
    }

    Configuration getConfiguration() {
      return configuration;
    }

    ImmutableList<? extends OptionInfo<?>> getOptionInfos() {
      return optionInfos;
    }
  }

  /**
   * Hydrates configured {@literal @CmdLine} arg fields and selects a desired set with the supplied
   * {@code filter}.
   *
   * @param configuration The configuration to find candidate {@literal @CmdLine} arg fields in.
   * @param filter A predicate to select fields with.
   * @return The desired hydrated {@literal @CmdLine} arg fields and optional {@literal @Positional}
   *     arg field.
   */
  static ArgsInfo fromConfiguration(Configuration configuration, Predicate<Field> filter) {
    Iterable<? extends OptionInfo<?>> optionInfos = Iterables.transform(
        filterFields(configuration.optionInfo(), filter), TO_OPTION_INFO);

    return new ArgsInfo(configuration, optionInfos);
  }

  private static Iterable<Field> filterFields(Iterable<ArgInfo> infos, Predicate<Field> filter) {
    return Iterables.filter(
        Optional.presentInstances(Iterables.transform(infos, TO_FIELD)),
        filter);
  }

  /**
   * Equivalent to calling {@code from(Predicates.alwaysTrue(), Arrays.asList(sources)}.
   */
  public static ArgsInfo from(Object... sources) throws IOException {
    return from(ImmutableList.copyOf(sources));
  }

  /**
   * Equivalent to calling {@code from(filter, Arrays.asList(sources)}.
   */
  public static ArgsInfo from(Predicate<Field> filter, Object... sources) throws IOException {
    return from(filter, ImmutableList.copyOf(sources));
  }

  /**
   * Equivalent to calling {@code from(Predicates.alwaysTrue(), sources}.
   */
  public static ArgsInfo from(Iterable<?> sources) throws IOException {
    return from(Predicates.<Field>alwaysTrue(), sources);
  }

  /**
   * Loads arg info from the given sources in addition to the default compile-time configuration.
   *
   * @param filter A predicate to select fields with.
   * @param sources Classes or object instances to scan for {@link Arg} fields.
   * @return The args info describing all discovered {@link Arg args}.
   * @throws IOException If there was a problem loading the default Args configuration.
   */
  public static ArgsInfo from(Predicate<Field> filter, Iterable<?> sources) throws IOException {
    Preconditions.checkNotNull(filter);
    Preconditions.checkNotNull(sources);

    Configuration configuration = Configuration.load();
    ArgsInfo staticInfo = Args.fromConfiguration(configuration, filter);

    final ImmutableSet.Builder<OptionInfo<?>> optionInfos =
        ImmutableSet.<OptionInfo<?>>builder().addAll(staticInfo.getOptionInfos());

    for (Object source : sources) {
      Class<?> clazz = source instanceof Class ? (Class) source : source.getClass();
      for (Field field : clazz.getDeclaredFields()) {
        if (filter.apply(field) && field.isAnnotationPresent(CmdLine.class)) {
          optionInfos.add(OptionInfo.createFromField(field, source));
        }
      }
    }

    return new ArgsInfo(configuration, optionInfos.build());
  }

  private Args() {
    // utility
  }
}
