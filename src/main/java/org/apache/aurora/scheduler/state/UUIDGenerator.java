package org.apache.aurora.scheduler.state;

import java.util.UUID;

/**
 * Wraps {@link java.util.UUID#randomUUID()} to facilitate unit testing.
 */
interface UUIDGenerator {
  UUID createNew();

  class UUIDGeneratorImpl implements UUIDGenerator {
    @Override
    public UUID createNew() {
      return UUID.randomUUID();
    }
  }
}
