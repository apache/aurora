package com.twitter.mesos.scheduler.httphandlers;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.thrift.TBase;

import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.common.thrift.Util;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.scheduler.CronJobManager;
import com.twitter.mesos.scheduler.LeaderRedirect;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

/**
 * Servlet that prints out the raw configuration for a specified struct.
 */
class StructDump extends StringTemplateServlet {

  private static final String ROLE_PARAM = "role";
  private static final String JOB_PARAM = "job";
  private static final String TASK_PARAM = "task";

  private final LeaderRedirect redirector;
  private final Storage storage;

  @Inject
  public StructDump(
      @CacheTemplates boolean cacheTemplates,
      Storage storage,
      LeaderRedirect redirector) {

    super("structdump", cacheTemplates);
    this.storage = Preconditions.checkNotNull(storage);
    this.redirector = Preconditions.checkNotNull(redirector);
  }

  @Override
  protected void doGet(final HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    Optional<String> leaderRedirect = redirector.getRedirectTarget(req);
    if (leaderRedirect.isPresent()) {
      resp.sendRedirect(leaderRedirect.get());
      return;
    }

    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        String role = req.getParameter(ROLE_PARAM);
        String job = req.getParameter(JOB_PARAM);
        final String taskId = req.getParameter(TASK_PARAM);

        String id;
        Work.Quiet<TBase> work;
        if ((role != null) && (job != null)) {
          final String key = Tasks.jobKey(role, job);
          id = "Cron job " + key;
          work = new Work.Quiet<TBase>() {
            @Override public TBase apply(StoreProvider storeProvider) {
              return storeProvider.getJobStore().fetchJob(CronJobManager.MANAGER_KEY, key);
            }
          };
        } else if (taskId != null) {
          id = "Task " + taskId;
          work = new Work.Quiet<TBase>() {
            @Override public TBase apply(StoreProvider storeProvider) {
              return Iterables.getOnlyElement(
                  storeProvider.getTaskStore().fetchTasks(Query.byId(taskId)), null);
            }
          };
        } else {
          template.setAttribute("exception", "Bad request - must specify task or role and job.");
          return;
        }

        template.setAttribute("id", id);
        TBase struct = storage.doInTransaction(work);
        if (struct == null) {
          template.setAttribute("exception", "Entity not found");
        } else {
          template.setAttribute("structPretty", Util.prettyPrint(struct));
        }
      }
    });
  }
}
