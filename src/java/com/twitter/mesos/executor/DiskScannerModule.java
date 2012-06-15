package com.twitter.mesos.executor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.executor.DiskUsageScanner.DiskUsageScannerImpl;

/**
 * Module to bind and run the disk usage scanner.
 */
public class DiskScannerModule extends AbstractModule {

  @CmdLine(name = "enable_disk_scanner",
      help = "Whether to scan for and kill tasks exceeding their disk allocation.")
  private static final Arg<Boolean> ENABLE_DISK_SCANNER = Arg.create(true);

  @CmdLine(name = "disk_scan_interval", help = "Interval to run the disk scanner on.")
  private static final Arg<Amount<Long, Time>> SCAN_DISK_INTERVAL =
      Arg.create(Amount.of(30L, Time.MINUTES));

  @Override
  protected void configure() {
    bind(DiskUsageScanner.class).to(DiskUsageScannerImpl.class);
    bind(DiskUsageScannerImpl.class).in(Singleton.class);

    if (ENABLE_DISK_SCANNER.get()) {
      LifecycleModule.bindStartupAction(binder(), DiskScannerLauncher.class);
    }
  }

  static class DiskScannerLauncher implements Command {
    private final DiskUsageScanner scanner;

    @Inject
    DiskScannerLauncher(DiskUsageScanner scanner) {
      this.scanner = Preconditions.checkNotNull(scanner);
    }

    @Override
    public void execute() {
      ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DiskUsageScanner-%d").build());

      long intervalSeconds = SCAN_DISK_INTERVAL.get().as(Time.SECONDS);
      executor.scheduleAtFixedRate(scanner, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }
  }
}
