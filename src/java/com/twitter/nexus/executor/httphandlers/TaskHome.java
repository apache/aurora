package com.twitter.nexus.executor.httphandlers;

import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.nexus.executor.ExecutorCore;
import com.twitter.nexus.executor.RunningTask;
import com.twitter.nexus.gen.TwitterTaskInfo;
import org.antlr.stringtemplate.StringTemplate;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * HTTP interface to view information about a task.
 *
 * @author wfarner
 */
public class TaskHome extends StringTemplateServlet {

  private static final String TASK_ID_PARAM = "task";

  @Inject private ExecutorCore executor;

  @Inject
  public TaskHome(@CacheTemplates boolean cacheTemplates) {
    super("taskhome", cacheTemplates);
  }

  @Override
  protected void doGet(final HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        String taskIdStr = req.getParameter(TASK_ID_PARAM);
        if (taskIdStr == null) {
          template.setAttribute("exception", "Task ID must be specified.");
          return;
        }

        int taskId;
        try {
          taskId = Integer.parseInt(taskIdStr);
        } catch (NumberFormatException e) {
          template.setAttribute("exception", "Invalid task id.");
          return;
        }

        RunningTask task = executor.getTask(taskId);
        if (task == null) {
          template.setAttribute("exception", "Task not found.");
          return;
        }

        TwitterTaskInfo taskInfo = task.getTask();
        template.setAttribute("taskInfo", taskInfo);

        template.setAttribute("leasedPorts", task.getLeasedPorts());
        template.setAttribute("taskDir", task.getSandboxDir());
      }
    });
  }
}
