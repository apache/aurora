package com.twitter.mesos.executor;

import java.io.File;

import com.google.common.base.Preconditions;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

/**
 * Determines the amount of different consumable resources that are being used by a process.
 *
 * @author William Farner
 */
public interface ResourceScanner {

  public ProcInfo getResourceUsage(int pid, File taskRoot);

  public static class ProcInfo {
    private int niceLevel;
    private Amount<Long, Data> vmSize;
    private Amount<Long, Data> diskUsed;

    private long jiffiesUsed;
    public int getNiceLevel() {
      return niceLevel;
    }

    public Amount<Long, Data> getVmSize() {
      return vmSize;
    }

    public Amount<Long, Data> getDiskUsed() {
      return diskUsed;
    }

    public long getJiffiesUsed() {
      return jiffiesUsed;
    }

    @Override
    public String toString() {
      return String.format("Nice: %d, VmSize: %s, Disk used: %s, Jiffies: %d",
          niceLevel, vmSize, diskUsed, jiffiesUsed);
    }

    public static Builder builder() {
      return new Builder();
    }

    public static class Builder {
      private final ProcInfo instance = new ProcInfo();

      public Builder setNiceLevel(int niceLevel) {
        instance.niceLevel = niceLevel;
        return this;
      }

      public Builder setVmSize(Amount<Long, Data> vmSize) {
        instance.vmSize = Preconditions.checkNotNull(vmSize);
        return this;
      }

      public Builder setDiskUaed(Amount<Long, Data> diskUsed) {
        instance.diskUsed = Preconditions.checkNotNull(diskUsed);
        return this;
      }

      public Builder setJiffiesUsed(long jiffiesUsed) {
        Preconditions.checkArgument(jiffiesUsed > 0);
        instance.jiffiesUsed = jiffiesUsed;
        return this;
      }

      public ProcInfo build() {
        return instance;
      }
    }
  }
}
