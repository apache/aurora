package com.twitter.mesos.executor.httphandlers;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.mesos.executor.ExecutorCore;

/**
 * HTTP interface for the executor.
 *
 * @author William Farner
 */
public class ExecutorHome extends StringTemplateServlet {

  private static final Logger LOG = Logger.getLogger(ExecutorHome.class.getName());

  private final ExecutorCore executor;

  @Inject
  public ExecutorHome(ExecutorCore executor, @CacheTemplates boolean cacheTemplates) {
    super("executorhome", cacheTemplates);
    this.executor = executor;
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

        template.setAttribute("tasks", ImmutableList.copyOf(executor.getTasks()));
      }
    });
  }
}

