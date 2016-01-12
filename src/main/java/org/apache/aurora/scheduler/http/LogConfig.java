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
package org.apache.aurora.scheduler.http;

import java.io.StringWriter;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import org.apache.aurora.common.util.templating.StringTemplateHelper;
import org.apache.aurora.common.util.templating.StringTemplateHelper.TemplateException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

/**
 * Servlet that allows for dynamic adjustment of the logging configuration.
 *
 * @author William Farner
 */
@Path("/logconfig")
public class LogConfig {
  private static final List<String> LOG_LEVELS = Lists.newArrayList(
      Level.OFF.levelStr,
      Level.ERROR.levelStr,
      Level.WARN.levelStr,
      Level.INFO.levelStr,
      Level.DEBUG.levelStr,
      Level.TRACE.levelStr,
      Level.ALL.levelStr,
      "INHERIT" // Display value for a null level, the logger inherits from its ancestor.
  );

  private final StringTemplateHelper template =
      new StringTemplateHelper(getClass(), "logconfig", false);

  @POST
  @Produces(MediaType.TEXT_HTML)
  public String post(
      @FormParam("logger") String loggerName,
      @FormParam("level") String loggerLevel) throws TemplateException {

    Optional<String> configChange = Optional.empty();
    if (loggerName != null && loggerLevel != null) {
      Logger logger = (Logger) LoggerFactory.getLogger(loggerName);
      Level newLevel = Level.toLevel(loggerLevel, null);
      logger.setLevel(newLevel);

      configChange = Optional.of(String.format("%s level changed to %s", loggerName, loggerLevel));
    }

    return displayPage(configChange);
  }

  @GET
  @Produces(MediaType.TEXT_HTML)
  public String get() throws TemplateException {
    return displayPage(Optional.empty());
  }

  protected String displayPage(Optional<String> configChange) throws TemplateException {
    StringWriter writer = new StringWriter();

    template.writeTemplate(writer, stringTemplate -> {
      if (configChange.isPresent()) {
        stringTemplate.setAttribute("configChange", configChange.get());
      }

      LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

      Set<LoggerConfig> loggers = FluentIterable.from(context.getLoggerList())
          .transform(logger -> new LoggerConfig(
              logger.getName(),
              Optional.ofNullable(logger.getLevel()).map(l -> l.levelStr).orElse("INHERIT")))
          .toSortedSet(Ordering.natural().onResultOf(LoggerConfig::getName));

      stringTemplate.setAttribute("loggers", loggers);
      stringTemplate.setAttribute("levels", LOG_LEVELS);
    });

    return writer.toString();
  }

  private static class LoggerConfig {
    private final String name;
    private final String level;

    LoggerConfig(String name, String level) {
      this.name = name;
      this.level = StringUtils.isBlank(level) ? "INHERIT" : level;
    }

    public String getName() {
      return name;
    }

    public String getLevel() {
      return level;
    }
  }
}
