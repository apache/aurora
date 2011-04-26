package com.twitter.mesos.scheduler.httphandlers;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import com.twitter.common.base.Closure;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.ClusterName;
import com.twitter.mesos.scheduler.SchedulerThriftInterface;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * Servlet to support job creation through the web interface.
 *
 * @author William Farner
 */
public class CreateJob extends StringTemplateServlet {

  private static final Logger LOG = Logger.getLogger(CreateJob.class.getName());

  private final SchedulerThriftInterface scheduler;
  private final String clusterName;

  @Inject
  CreateJob(@CacheTemplates boolean cacheTemplates,
      SchedulerThriftInterface scheduler,
      @ClusterName String clusterName) {
    super("create_job", cacheTemplates);
    this.clusterName = checkNotBlank(clusterName);
    this.scheduler = checkNotNull(scheduler);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    writeTemplate(resp, new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        template.setAttribute("cluster_name", clusterName);
      }
    });
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    resp.setContentType("application/json");

    JobConfiguration newJob = getJobFromRequest(req);
    LOG.info("Job created through web UI: " + newJob);
    CreateJobResponse response = scheduler.createJob(newJob);

    OutputStream responseBody = resp.getOutputStream();

    resp.setStatus(HttpServletResponse.SC_OK);
    String result = (response.responseCode == ResponseCode.OK) ? "success" : "error";
    try {
      IOUtils.write(new Gson().toJson(
          ImmutableMap.of("result", result, "message", response.getMessage())), responseBody);
    } finally {
      responseBody.close();
    }
  }

  private static JobConfiguration getJobFromRequest(HttpServletRequest req) {
    JobConfiguration job = new JobConfiguration();
    job.setOwner(req.getParameter("job_owner"));
    job.setName(req.getParameter("job_name"));
    job.setCronSchedule(req.getParameter("cron_schedule"));

    String cronCollisionPolicy = req.getParameter("cron_collision_policy");
    if (!StringUtils.isBlank(cronCollisionPolicy)) {
      job.setCronCollisionPolicy(CronCollisionPolicy.valueOf(cronCollisionPolicy));
    }

    final TwitterTaskInfo task = new TwitterTaskInfo()
        .setConfiguration(ImmutableMap.<String, String>builder()
        .put("hdfs_path", req.getParameter("hdfs_path"))
        .put("start_command", req.getParameter("start_command"))
        .put("num_cpus", req.getParameter("num_cpus"))
        .put("ram_mb", req.getParameter("ram_mb"))
        .put("max_task_failures", req.getParameter("max_task_failures"))
        .put("max_per_host", req.getParameter("max_per_host"))
        .put("disk_mb", req.getParameter("disk_mb"))
        .put("daemon", req.getParameter("is_daemon"))
        .put("avoid_jobs", req.getParameter("avoid_jobs"))
        .build());

    int instances = 1;
    String instancesStr = req.getParameter("num_instances");
    if (!StringUtils.isBlank(instancesStr)) {
      instances = Integer.parseInt(instancesStr);
    }

    int shardId = 0;
    for (int i = 0; i < instances; i++) {
      job.addToTaskConfigs(task.deepCopy().setShardId(shardId++));
    }

    return job;
  }
}
