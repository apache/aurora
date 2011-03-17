package com.twitter.mesos.scheduler.httphandlers;

import com.google.inject.Binder;

import com.twitter.common.application.http.Registration;

/**
 * Utility class to register HTTP resources for mesos dashboards.
 *
 * @author William Farner
 */
public class HttpAssets {

  public static void register(Binder binder) {
    Registration.registerHttpAsset(binder, "/js/mootools-core.js", HttpAssets.class,
        "js/mootools-core.js", "application/javascript", true);
    Registration.registerHttpAsset(binder, "/js/mootools-more.js", HttpAssets.class,
        "js/mootools-more.js", "application/javascript", true);
    Registration.registerHttpAsset(binder, "/js/tit.js", HttpAssets.class,
        "js/tit.js", "application/javascript", true);
    Registration.registerHttpAsset(binder, "/css/global.css", HttpAssets.class,
        "css/global.css", "text/css", true);
  }

  private HttpAssets() {
    // Utility.
  }
}
