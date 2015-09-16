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
package org.apache.aurora.common.net.http.handlers;

import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * HTTP handler to print the stacks of all live threads.
 *
 * @author William Farner
 */
@Path("/threads")
public class ThreadStackPrinter {
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getThreadStacks() {
    List<String> lines = Lists.newLinkedList();
    for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
      Thread t = entry.getKey();
      lines.add(String.format("Name: %s\nState: %s\nDaemon: %s\nID: %d",
          t.getName(), t.getState(), t.isDaemon(), t.getId()));
      for (StackTraceElement s : entry.getValue()) {
        lines.add("    " + s.toString());
      }
    }
    return Joiner.on("\n").join(lines);
  }
}
