package com.twitter.mesos.scheduler;

import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.Scalar;
import org.apache.mesos.Protos.Resource.Type;

/**
 * Utility class for maintaining a single declaration point for resource names.
 *
 * @author William Farner
 */
public class Resources {

  public static final String CPUS = "cpus";
  public static final String RAM_MB = "mem";

  private Resources() {
    // Utility.
  }

  static final Resource makeResource(String name, double value) {
    return Resource.newBuilder().setName(name).setType(Type.SCALAR)
        .setScalar(Scalar.newBuilder().setValue(value)).build();
  }
}
