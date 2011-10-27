package com.twitter.mesos.scheduler.httphandlers;

import java.net.URL;

import com.google.common.io.Resources;
import com.google.inject.AbstractModule;

import com.twitter.common.application.http.Registration;


/**
 *  Static image asset wrapper for the Mesos Scheduler web interface.
 */
public final class ImageAssets {
  private static final String CONTENT_TYPE_IMAGE_JPEG = "image/jpeg";
  private static final String CONTENT_TYPE_IMAGE_PNG = "image/png";

  private ImageAssets() {
    // Utility
  }

  /**
   * Contains all the assets included in the package.
   */
  enum Asset {
    THERMOS_ICON("assets/thermos.png", CONTENT_TYPE_IMAGE_PNG);

    private final String relativePath;
    private final String contentType;

    Asset(String relativePath, String contentType) {
      this.relativePath = relativePath;
      this.contentType = contentType;
    }

    URL getUrl() {
      return Resources.getResource(ImageAssets.class, relativePath);
    }

    String getContentType() {
      return contentType;
    }

    String getRelativePath() {
      return relativePath;
    }
  }

  /**
   * Registers all the assets.
   */
  public static final class HttpAssetModule extends AbstractModule {
    @Override
    protected void configure() {
      for (Asset asset : Asset.values()) {
        Registration.registerHttpAsset(binder(),
            "/" + asset.getRelativePath(),
            asset.getUrl(),
            asset.getContentType(),
            true);
      }
    }
  }
}
