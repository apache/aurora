package com.twitter.mesos.scheduler.httphandlers;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Optional;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.mesos.scheduler.ClusterName;
import com.twitter.mesos.scheduler.LeaderRedirect;
import com.twitter.mesos.scheduler.MesosSchedulerImpl.SlaveHosts;
import com.twitter.mesos.scheduler.sync.ExecutorWatchdog;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * HTTP interface to serve as a HUD for the mesos executors tracked in the scheduler.
 *
 * @author Benjamin Mahler
 */
public class ExecutorsHome extends StringTemplateServlet {
  private final String clusterName;
  private final ExecutorWatchdog watchdog;
  private final SlaveHosts slaveHosts;
  private LeaderRedirect redirector;

  /**
   * Injected constructor.
   *
   * @param cacheTemplates whether to cache templates
   * @param clusterName cluster name
   * @param watchdog the executor watchdog
   * @param slaveHosts executor slave hosts
   * @param redirector leader redirector
   */
  @Inject
  public ExecutorsHome(@CacheTemplates boolean cacheTemplates, @ClusterName String clusterName,
      ExecutorWatchdog watchdog, SlaveHosts slaveHosts, LeaderRedirect redirector) {
    super("executors", cacheTemplates);
    this.clusterName = checkNotBlank(clusterName);
    this.watchdog = checkNotNull(watchdog);
    this.slaveHosts = checkNotNull(slaveHosts);
    this.redirector = checkNotNull(redirector);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    Optional<String> leaderRedirect = redirector.getRedirectTarget(req);
    if (leaderRedirect.isPresent()) {
      resp.sendRedirect(leaderRedirect.get());
      return;
    }

    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        template.setAttribute("cluster_name", clusterName);
        template.setAttribute("executorPositions", watchdog.getExecutorPositions());
        template.setAttribute("slaves", slaveHosts.getSlaves());
      }
    });
  }
}
