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

package org.apache.aurora.scheduler.config;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import javax.security.auth.kerberos.KerberosPrincipal;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.IStringConverterFactory;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.app.SchedulerMain;
import org.apache.aurora.scheduler.app.VolumeConverter;
import org.apache.aurora.scheduler.config.converters.ClassConverter;
import org.apache.aurora.scheduler.config.converters.DataAmountConverter;
import org.apache.aurora.scheduler.config.converters.DockerParameterConverter;
import org.apache.aurora.scheduler.config.converters.InetSocketAddressConverter;
import org.apache.aurora.scheduler.config.converters.TimeAmountConverter;
import org.apache.aurora.scheduler.config.types.DataAmount;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.http.api.security.KerberosPrincipalConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses command line options and populates {@link CliOptions}.
 */
public final class CommandLine {

  private static final Logger LOG = LoggerFactory.getLogger(CommandLine.class);

  // TODO(wfarner): This can go away if/when options are no longer accessed statically.
  private static CliOptions instance = null;

  private static List<Object> customOptions = Lists.newArrayList();

  private CommandLine() {
    // Utility class.
  }

  /**
   * Similar to {@link #initializeForTest()}, but resets the class to an un-parsed state.
   */
  @VisibleForTesting
  static void clearForTest() {
    instance = null;
    customOptions = Lists.newArrayList();
  }

  /**
   * Initializes static command line state - the static parsed instance, and custom options objects.
   */
  @VisibleForTesting
  public static void initializeForTest() {
    instance = new CliOptions();
    customOptions = Lists.newArrayList();
  }

  private static JCommander prepareParser(CliOptions options) {
    JCommander.Builder builder = JCommander.newBuilder()
        .programName(SchedulerMain.class.getName());

    builder.addConverterFactory(new IStringConverterFactory() {
      private Map<Class<?>, Class<? extends IStringConverter<?>>> classConverters =
          ImmutableMap.<Class<?>, Class<? extends IStringConverter<?>>>builder()
              .put(Class.class, ClassConverter.class)
              .put(DataAmount.class, DataAmountConverter.class)
              .put(DockerParameter.class, DockerParameterConverter.class)
              .put(InetSocketAddress.class, InetSocketAddressConverter.class)
              .put(KerberosPrincipal.class, KerberosPrincipalConverter.class)
              .put(TimeAmount.class, TimeAmountConverter.class)
              .put(Volume.class, VolumeConverter.class)
              .build();

      @SuppressWarnings("unchecked")
      @Override
      public <T> Class<? extends IStringConverter<T>> getConverter(Class<T> forType) {
        return (Class<IStringConverter<T>>) classConverters.get(forType);
      }
    });

    builder.addObject(getOptionsObjects(options));
    return builder.build();
  }

  /**
   * Applies arg values to the options object.
   *
   * @param args Command line arguments.
   */
  @VisibleForTesting
  public static CliOptions parseOptions(String... args) {
    JCommander parser = null;
    try {
      parser = prepareParser(new CliOptions(ImmutableList.copyOf(customOptions)));

      // We first perform a 'dummy' parsing round.  This induces classloading on any third-party
      // code, where they can statically invoke registerCustomOptions().
      parser.setAcceptUnknownOptions(true);
      parser.parseWithoutValidation(args);

      CliOptions options = new CliOptions(ImmutableList.copyOf(customOptions));
      parser = prepareParser(options);
      parser.parse(args);
      instance = options;
      return options;
    } catch (ParameterException e) {
      if (parser != null) {
        parser.usage();
      }
      LOG.error(e.getMessage());
      System.exit(1);
      throw new RuntimeException(e);
    } catch (RuntimeException e) {
      throw e;
    }
  }

  /**
   * Gets the static and globally-accessible CLI options.  This exists only to support legacy use
   * cases that cannot yet support injected arguments.  New callers should not be added.
   *
   * @return global options
   */
  public static CliOptions legacyGetStaticOptions() {
    if (instance == null) {
      throw new IllegalStateException("Attempted to fetch command line arguments before parsing.");
    }
    return instance;
  }

  /**
   * Registers a custom options container for inclusion during command line option parsing.  This
   * is useful to allow third-party modules to include custom command line options.
   *
   * @param options Custom options object.
   *                See {@link com.beust.jcommander.JCommander.Builder#addObject(Object)} for
   *                details.
   */
  public static void registerCustomOptions(Object options) {
    Preconditions.checkState(
        instance == null,
        "Attempted to register custom options after command line parsing.");

    customOptions.add(options);
  }

  @VisibleForTesting
  static List<Object> getOptionsObjects(CliOptions options) {
    ImmutableList.Builder<Object> objects = ImmutableList.builder();

    // Reflect on fields defined in CliOptions to DRY and avoid mistakes of forgetting to add an
    // option field here.
    for (Field field : CliOptions.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }

      try {
        if (Iterable.class.isAssignableFrom(field.getType())) {
          Iterable<?> iterableValue = (Iterable<?>) field.get(options);
          objects.addAll(iterableValue);
        } else {
          objects.add(field.get(options));
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    return objects.build();
  }
}
