/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.http.api;

import java.util.function.Function;

import javax.servlet.ServletContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobSummary;
import org.apache.aurora.gen.JobSummaryResult;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.Result;
import org.apache.aurora.gen.RoleSummary;
import org.apache.aurora.gen.RoleSummaryResult;
import org.apache.aurora.gen.ScheduleStatusResult;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.http.AbstractJettyTest;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IResponse;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ResponseCode.OK;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class ApiBetaTest extends AbstractJettyTest {
  private AnnotatedAuroraAdmin thrift;

  @Before
  public void setUp() {
    thrift = createMock(AnnotatedAuroraAdmin.class);
  }

  @Override
  protected Function<ServletContext, Module> getChildServletModule() {
    return (servletContext) -> Modules.combine(
        new ApiModule(new ApiModule.Options()),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(AnnotatedAuroraAdmin.class).toInstance(thrift);
          }
        }
    );
  }

  private static final ITaskConfig TASK_CONFIG = TaskTestUtil.makeConfig(TaskTestUtil.JOB);
  private static final IJobConfiguration JOB_CONFIG = IJobConfiguration.build(
      new JobConfiguration()
          .setCronCollisionPolicy(CronCollisionPolicy.CANCEL_NEW)
          .setKey(new JobKey("role", "env", "name"))
          .setTaskConfig(TASK_CONFIG.newBuilder()));

  @Test
  public void testCreateJob() throws Exception {
    Response response = new Response()
        .setResponseCode(OK);

    JobConfiguration job = JOB_CONFIG.newBuilder();
    expect(thrift.createJob(anyObject())).andReturn(response);

    replayAndStart();

    Response actualResponse = getRequestBuilder("/apibeta/createJob")
        .entity(
            ImmutableMap.of("description", job),
            MediaType.APPLICATION_JSON)
        .post(Response.class);
    assertEquals(IResponse.build(response), IResponse.build(actualResponse));
  }

  @Test
  public void testGetRoleSummary() throws Exception {
    Response response = new Response()
        .setResponseCode(OK)
        .setResult(Result.roleSummaryResult(new RoleSummaryResult()
            .setSummaries(ImmutableSet.of(new RoleSummary()
                .setCronJobCount(1)
                .setJobCount(2)
                .setRole("role")))));

    expect(thrift.getRoleSummary()).andReturn(response);

    replayAndStart();

    Response actualResponse = getRequestBuilder("/apibeta/getRoleSummary")
        .post(Response.class);
    assertEquals(response, actualResponse);
  }

  @Test
  public void testGetJobSummary() throws Exception {
    Response response = new Response()
        .setResponseCode(OK)
        .setResult(Result.jobSummaryResult(new JobSummaryResult()
            .setSummaries(ImmutableSet.of(new JobSummary()
                .setJob(JOB_CONFIG.newBuilder())))));

    expect(thrift.getJobSummary("roleA")).andReturn(response);

    replayAndStart();

    Response actualResponse = getRequestBuilder("/apibeta/getJobSummary")
        .entity(ImmutableMap.of("role", "roleA"), MediaType.APPLICATION_JSON)
        .post(Response.class);
    assertEquals(IResponse.build(response), IResponse.build(actualResponse));
  }

  @Test
  public void testGetTasks() throws Exception {
    ScheduledTask task = new ScheduledTask()
        .setStatus(RUNNING)
        .setAssignedTask(
            new AssignedTask()
                .setTask(TASK_CONFIG.newBuilder()));
    Response response = new Response()
        .setResponseCode(OK)
        .setResult(Result.scheduleStatusResult(new ScheduleStatusResult()
            .setTasks(ImmutableList.of(task))));

    TaskQuery query = new TaskQuery()
        .setStatuses(ImmutableSet.of(RUNNING))
        .setTaskIds(ImmutableSet.of("a"));

    expect(thrift.getTasksStatus(query)).andReturn(response);

    replayAndStart();

    Response actualResponse = getRequestBuilder("/apibeta/getTasksStatus")
        .entity(ImmutableMap.of("query", query), MediaType.APPLICATION_JSON)
        .post(Response.class);
    assertEquals(IResponse.build(response), IResponse.build(actualResponse));
  }

  @Test
  public void testGetHelp() throws Exception {
    replayAndStart();

    ClientResponse response = getRequestBuilder("/apibeta")
        .accept(MediaType.TEXT_HTML)
        .get(ClientResponse.class);
    assertEquals(Status.SEE_OTHER.getStatusCode(), response.getStatus());
  }

  @Test
  public void testPostInvalidStructure() throws Exception {
    replayAndStart();

    ClientResponse badRequest = getRequestBuilder("/apibeta/createJob")
        .entity("not an object", MediaType.APPLICATION_JSON)
        .post(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), badRequest.getStatus());

    ClientResponse badParameter = getRequestBuilder("/apibeta/createJob")
        .entity(ImmutableMap.of("description", "not a job description"), MediaType.APPLICATION_JSON)
        .post(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), badParameter.getStatus());
  }

  @Test
  public void testInvalidApiMethod() throws Exception {
    replayAndStart();

    ClientResponse response = getRequestBuilder("/apibeta/notAMethod")
        .post(ClientResponse.class);
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testPostInvalidJson() throws Exception {
    replayAndStart();

    ClientConfig config = new DefaultClientConfig();
    Client client = Client.create(config);
    ClientResponse response = client.resource(makeUrl("/apibeta/createJob"))
        .accept(MediaType.APPLICATION_JSON)
        .entity("{this is bad json}", MediaType.APPLICATION_JSON)
        .post(ClientResponse.class);
    assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }
}
