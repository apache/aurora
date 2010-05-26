package com.twitter.nexus.scheduler.httphandlers;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.nexus.gen.JobConfiguration;
import com.twitter.nexus.gen.TaskQuery;
import com.twitter.nexus.gen.TrackedTask;
import com.twitter.nexus.scheduler.CronJobManager;
import com.twitter.nexus.scheduler.SchedulerCore;
import it.sauronsoftware.cron4j.Predictor;
import org.antlr.stringtemplate.StringTemplate;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * HTTP interface to provide information about jobs for a specific nexus user.
 *
 * @author wfarner
 */
public class SchedulerzUser extends StringTemplateServlet {
  @Inject
  private SchedulerCore scheduler;

  @Inject
  private CronJobManager cronScheduler;

  private static final String USER_PARAM = "user";

  @Inject
  public SchedulerzUser(@CacheTemplates boolean cacheTemplates) {
    super("schedulerzuser", cacheTemplates);
  }

  @Override
  protected void doGet(final HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        final String user = req.getParameter(USER_PARAM);
        if (user == null) {
          template.setAttribute("exception", "Please specify a user.");
          return;
        }

        template.setAttribute("user", user);

        TaskQuery query = new TaskQuery().setOwner(user);

        Map<String, Job> jobs = Maps.newHashMap();

        for (TrackedTask task : scheduler.getTasks(query)) {
          Job job = jobs.get(task.getJobName());
          if (job == null) {
            job = new Job();
            job.name = task.getJobName();
            jobs.put(job.name, job);
          }

          job.taskCount++;
        }

        template.setAttribute("jobs", jobs.values());

        Iterable<JobConfiguration> cronJobs = Iterables.filter(
            cronScheduler.getJobs(), new Predicate<JobConfiguration>() {
              @Override public boolean apply(JobConfiguration job) {
                return job.getOwner().equals(user);
              }
            });
        Iterable<CronJob> cronJobObjs = Iterables.transform(cronJobs,
            new Function<JobConfiguration, CronJob>() {
              @Override public CronJob apply(JobConfiguration job) {
                CronJob cronJob = new CronJob();
                cronJob.name = job.getName();
                cronJob.taskCount = job.getTaskConfigsSize();
                cronJob.cronSchedule = job.getCronSchedule();
                cronJob.nextRun = new Predictor(cronJob.cronSchedule).nextMatchingDate().toString();
                return cronJob;
              }
        });

        template.setAttribute("cronJobs", Lists.newArrayList(cronJobObjs));
      }
    });
  }

  class Job {
    String name;
    int taskCount;

    public String getName() {
      return name;
    }

    public int getTaskCount() {
      return taskCount;
    }
  }

  class CronJob extends Job {
    String cronSchedule;
    String nextRun;

    public String getCronSchedule() {
      return cronSchedule;
    }

    public String getNextRun() {
      return nextRun;
    }
  }
}
