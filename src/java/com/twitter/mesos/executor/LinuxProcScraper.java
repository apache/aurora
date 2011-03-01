package com.twitter.mesos.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A wrapper class to interact with the /proc filesystem in linux and gather information about a
 * process.
 *
 * @author William Farner
 */
public class LinuxProcScraper implements ResourceScanner {

  private static Logger LOG = Logger.getLogger(LinuxProcScraper.class.getName());

  private static final File DEFAULT_PROC_DIR = new File("/proc");

  private final File procDir;

  public LinuxProcScraper() {
    this(DEFAULT_PROC_DIR);
  }

  @VisibleForTesting
  LinuxProcScraper(File procDir) {
    this.procDir = Preconditions.checkNotNull(procDir);
    Preconditions.checkArgument(procDir.exists(),
        "Provided proc filesystem dir does not exist: " + procDir);
  }

  @Override
  public ProcInfo getResourceUsage(int pid, File taskRoot) {
    Preconditions.checkNotNull(pid);
    Preconditions.checkArgument(pid > 0, "Invalid process id: " + pid);

    File processDir = new File(procDir, String.valueOf(pid));
    if (!processDir.exists()) {
      LOG.warning("No proc dir for process: " + pid);
      return null;
    }

    File statFile = new File(processDir, "stat");
    ProcInfo.Builder infoBuilder;
    try {
      infoBuilder = parse(Joiner.on("").join(Files.readLines(statFile, Charsets.UTF_8)));
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Failed to read stat file " + statFile, e);
      return null;
    }

    return infoBuilder
        .setDiskUaed(Amount.of(FileUtils.sizeOfDirectory(taskRoot), Data.BYTES))
        .build();
  }

  @VisibleForTesting
  ProcInfo.Builder parse(String procStatData) {
    String[] statFields = procStatData.split(" ");
    if (statFields.length < 23) {
      LOG.warning("Too few fields in process stat data: " + procStatData);
      return null;
    }

    try {
      // Field 14 is the user-mode jiffies:
      //     utime %lu The number of jiffies that this process has been scheduled in user mode.
      long userModeJiffies = Long.parseLong(statFields[13]);

      // Field 15 is the kernel-mode jiffies.
      //     stime %lu The number of jiffies that this process has been scheduled in kernel mode.
      long kernelModeJiffies = Long.parseLong(statFields[14]);

      // Field 19 is the nice level:
      //     nice %ld The nice value ranges from 19 (nicest) to -19 (not nice to others).
      int nice = Integer.parseInt(statFields[18]);

      // Field 23 is the VM size:
      //     vsize %lu Virtual memory size in bytes.
      Amount<Long, Data> vmSize = Amount.of(Long.parseLong(statFields[22]), Data.BYTES);

      return ProcInfo.builder()
          .setNiceLevel(nice)
          .setVmSize(vmSize)
          .setJiffiesUsed(userModeJiffies + kernelModeJiffies);
    } catch (NumberFormatException e) {
      LOG.log(Level.WARNING, "Invalid formatting of field.", e);
      return null;
    }
  }
}
