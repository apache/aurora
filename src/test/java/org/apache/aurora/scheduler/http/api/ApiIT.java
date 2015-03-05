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

import javax.ws.rs.core.HttpHeaders;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.sun.jersey.api.client.ClientResponse;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.Response;
import org.apache.aurora.scheduler.http.JettyServerModuleTest;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class ApiIT extends JettyServerModuleTest {
  private AuroraAdmin.Iface thrift;

  @Before
  public void setUp() {
    thrift = createMock(AuroraAdmin.Iface.class);
  }

  @Override
  protected Module getChildServletModule() {
    return Modules.combine(
        new ApiModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(AuroraAdmin.Iface.class).toInstance(thrift);
          }
        });
  }

  @Test
  public void testGzipFilterApplied() throws Exception {
    expect(thrift.getRoleSummary()).andReturn(new Response());

    replayAndStart();

    ClientResponse response = getRequestBuilder(ApiModule.API_PATH)
        .header(HttpHeaders.ACCEPT_ENCODING, "gzip")
        .type("application/x-thrift")
        .post(ClientResponse.class, "[1,\"getRoleSummary\",1,0,{}]");

    assertEquals("gzip", response.getHeaders().getFirst(HttpHeaders.CONTENT_ENCODING));
  }
}
