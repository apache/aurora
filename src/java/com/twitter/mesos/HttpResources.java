package com.twitter.mesos;

import com.google.inject.Binder;

import com.twitter.common.process.GuicedProcess;

/**
 * Utility class to register HTTP resources for mesos dashboards.
 *
 * @author wfarner
 */
public class HttpResources {

  public static void register(Binder binder) {
    GuicedProcess.registerHttpResource(binder, "/js/mootools-core.js", HttpResources.class,
        "js/mootools-core.js", "application/javascript", true);
    GuicedProcess.registerHttpResource(binder, "/js/mootools-more.js", HttpResources.class,
        "js/mootools-more.js", "application/javascript", true);
    GuicedProcess.registerHttpResource(binder, "/js/tit.js", HttpResources.class,
        "js/tit.js", "application/javascript", true);
    GuicedProcess.registerHttpResource(binder, "/css/global.css", HttpResources.class,
        "css/global.css", "text/css", true);
  }

  private HttpResources() {
    // Utility.
  }
}
