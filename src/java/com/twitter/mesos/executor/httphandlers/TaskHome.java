package com.twitter.mesos.executor.httphandlers;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.common.thrift.Util;
import com.twitter.mesos.executor.ExecutorCore;
import com.twitter.mesos.executor.Task;

/**
 * HTTP interface to view information about a task.
 *
 * @author William Farner
 */
public class TaskHome extends StringTemplateServlet {

  private static final String TASK_ID_PARAM = "task";

  private final ExecutorCore executor;

  @Inject
  public TaskHome(ExecutorCore executor, @CacheTemplates boolean cacheTemplates) {
    super("taskhome", cacheTemplates);
    this.executor = executor;
  }

  @Override
  protected void doGet(final HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        String taskId = req.getParameter(TASK_ID_PARAM);
        if (taskId == null) {
          template.setAttribute("exception", "Task ID must be specified.");
          return;
        }

        Task task = executor.getTask(taskId);
        if (task == null) {
          template.setAttribute("exception", "Task not found.");
          return;
        }
        template.setAttribute("id", task.getId());
        template.setAttribute("taskDir", task.getSandboxDir());
        template.setAttribute("status", task.getScheduleStatus());
        template.setAttribute("taskPretty", Util.prettyPrint(task.getAssignedTask()));
      }
    });
  }
}
