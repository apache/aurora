package com.twitter.mesos.scheduler.storage.db;

import java.io.IOException;
import java.net.URL;
import java.util.logging.Logger;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Utilities for dealing with DbStorage operations.
 *
 * @author John Sirois
 */
public final class DbStorageUtil {

  private static final Logger LOG = Logger.getLogger(DbStorageUtil.class.getName());

  private DbStorageUtil() {
    // utility
  }

  /**
   * Executes the sql defined in a resource file against the database covered by jdbcTemplate.
   *
   * @param jdbcTemplate The {@code JdbcTemplate} object to execute database operation against.
   * @param contextClass A class whose package will be used as the base to search for the
   *     {@code sqlResourcePath}.
   * @param sqlResourcePath The path to resource relative to the package of {@code contextClass}.
   */
  public static void executeSql(JdbcTemplate jdbcTemplate, Class<?> contextClass,
      String sqlResourcePath, boolean logSql) {

    URL sqlUrl = Resources.getResource(contextClass, sqlResourcePath);
    jdbcTemplate.execute(String.format("RUNSCRIPT FROM '%s'", sqlUrl));
    if (logSql) {
      try {
        LOG.info(Resources.toString(sqlUrl, Charsets.UTF_8));
      } catch (IOException e) {
        LOG.warning("Failed to log sql that was successfully applied to db from: " + sqlUrl);
      }
    }
  }
}
