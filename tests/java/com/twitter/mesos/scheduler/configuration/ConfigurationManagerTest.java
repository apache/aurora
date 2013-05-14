package com.twitter.mesos.scheduler.configuration;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.twitter.mesos.gen.test.Constants.INVALID_IDENTIFIERS;
import static com.twitter.mesos.gen.test.Constants.VALID_IDENTIFIERS;

import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.isGoodIdentifier;

public class ConfigurationManagerTest {
  @Test
  public void testIsGoodIdentifier() {
    for (String identifier : VALID_IDENTIFIERS) {
      assertTrue(isGoodIdentifier(identifier));
    }
    for (String identifier : INVALID_IDENTIFIERS) {
      assertFalse(isGoodIdentifier(identifier));
    }
  }
}
