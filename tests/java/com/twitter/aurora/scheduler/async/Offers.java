package com.twitter.aurora.scheduler.async;

import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;

/**
 * Utility class for creating resource offers.
 */
final class Offers {
  private Offers() {
    // Utility class.
  }

  static final String DEFAULT_HOST = "hostname";

  static Offer makeOffer(String offerId) {
    return Offers.makeOffer(offerId, DEFAULT_HOST);
  }

  static Offer makeOffer(String offerId, String hostName) {
    return Offer.newBuilder()
        .setId(OfferID.newBuilder().setValue(offerId))
        .setFrameworkId(FrameworkID.newBuilder().setValue("framework_id"))
        .setSlaveId(SlaveID.newBuilder().setValue("slave_id-" + offerId))
        .setHostname(hostName)
        .build();
  }
}
