package com.twitter.mesos.scheduler.db;

import java.io.IOException;
import java.net.URL;
import java.util.logging.Logger;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;

import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Utilities for dealing with database operations.
 *
 * @author John Sirois
 */
public final class DbUtil {

  private static final Logger LOG = Logger.getLogger(DbUtil.class.getName());

  private DbUtil() {
    // utility
  }

  /**
   * Executes the sql defined in a resource file against the database covered by jdbcTemplate.
   *
   * @param jdbcTemplate The jdbc template object to execute database operation against.
   * @param sqlResource A handle to the resource contianing the sql to execute.
   * @param logSql {@code true} to log the applied sql
   * @throws IllegalArgumentException if the given sql resource does not exist
   */
  public static void executeSql(JdbcTemplate jdbcTemplate, ClassPathResource sqlResource,
      boolean logSql) {

    URL sqlResourceUrl = getResourceURL(sqlResource);
    jdbcTemplate.execute(String.format("RUNSCRIPT FROM '%s'", sqlResourceUrl));
    if (logSql) {
      try {
        LOG.info(Resources.toString(sqlResourceUrl, Charsets.UTF_8));
      } catch (IOException e) {
        LOG.warning("Failed to log sql that was successfully applied to db from: " + sqlResourceUrl);
      }
    }
  }

  private static URL getResourceURL(ClassPathResource resource) {
    Preconditions.checkArgument(resource.exists());

    try {
      return resource.getURL();
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unexpected problem obtaining URL for existing resource: " + resource);
    }
  }
}
