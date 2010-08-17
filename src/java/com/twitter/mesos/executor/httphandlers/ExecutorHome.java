package com.twitter.mesos.executor.httphandlers;

import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.mesos.executor.ExecutorCore;
import org.antlr.stringtemplate.StringTemplate;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HTTP interface for the executor.
 *
 * @author wfarner
 */
public class ExecutorHome extends StringTemplateServlet {

  private static final Logger LOG = Logger.getLogger(ExecutorHome.class.getName());

  @Inject
  private ExecutorCore executor;

  @Inject
  public ExecutorHome(@CacheTemplates boolean cacheTemplates) {
    super("executorhome", cacheTemplates);
  }

  @Override
  protected void doGet(final HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        try {
          template.setAttribute("hostname", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
          LOG.log(Level.SEVERE, "Failed to look up self hostname.", e);
        }

        template.setAttribute("tasks", executor.getTasks());
      }
    });
  }
}

