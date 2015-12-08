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
package org.apache.aurora.common.net.http.handlers;

import java.io.StringWriter;
import java.util.List;
import java.util.Optional;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.LoggingMXBean;

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.aurora.common.base.Closure;
import org.apache.aurora.common.util.templating.StringTemplateHelper;
import org.apache.aurora.common.util.templating.StringTemplateHelper.TemplateException;
import org.apache.commons.lang.StringUtils;

/**
 * Servlet that allows for dynamic adjustment of the logging configuration.
 *
 * @author William Farner
 */
@Path("/logconfig")
public class LogConfig {
  private static final List<String> LOG_LEVELS = Lists.newArrayList(
      Level.SEVERE.getName(),
      Level.WARNING.getName(),
      Level.INFO.getName(),
      Level.CONFIG.getName(),
      Level.FINE.getName(),
      Level.FINER.getName(),
      Level.FINEST.getName(),
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
      Logger logger = Logger.getLogger(loggerName);
      Level newLevel = loggerLevel.equals("INHERIT") ? null : Level.parse(loggerLevel);
      logger.setLevel(newLevel);
      if (newLevel != null) {
        maybeAdjustHandlerLevels(logger, newLevel);
      }

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
      LoggingMXBean logBean = LogManager.getLoggingMXBean();

      if (configChange.isPresent()) {
        stringTemplate.setAttribute("configChange", configChange.get());
      }

      List<LoggerConfig> loggerConfigs = Lists.newArrayList();
      for (String logger : Ordering.natural().immutableSortedCopy(logBean.getLoggerNames())) {
        loggerConfigs.add(new LoggerConfig(logger, logBean.getLoggerLevel(logger)));
      }

      stringTemplate.setAttribute("loggers", loggerConfigs);
      stringTemplate.setAttribute("levels", LOG_LEVELS);
    });

    return writer.toString();
  }

  private void maybeAdjustHandlerLevels(Logger logger, Level newLevel) {
    do {
      for (Handler handler : logger.getHandlers()) {
        Level handlerLevel = handler.getLevel();
        if (newLevel.intValue() < handlerLevel.intValue()) {
          handler.setLevel(newLevel);
        }
      }
    } while (logger.getUseParentHandlers() && (logger = logger.getParent()) != null);
  }

  private class LoggerConfig {
    private final String name;
    private final String level;

    public LoggerConfig(String name, String level) {
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
