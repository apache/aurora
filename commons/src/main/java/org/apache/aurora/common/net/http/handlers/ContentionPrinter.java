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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

/**
 * HTTP request handler that prints information about blocked threads.
 *
 * @author William Farner
 */
@Path("/contention")
public class ContentionPrinter {
  public ContentionPrinter() {
    ManagementFactory.getThreadMXBean().setThreadContentionMonitoringEnabled(true);
  }

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getContention() {
    List<String> lines = Lists.newLinkedList();
    ThreadMXBean bean = ManagementFactory.getThreadMXBean();

    Map<Long, StackTraceElement[]> threadStacks = Maps.newHashMap();
    for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
      threadStacks.put(entry.getKey().getId(), entry.getValue());
    }

    Set<Long> lockOwners = Sets.newHashSet();

    lines.add("Locked threads:");
    for (ThreadInfo t : bean.getThreadInfo(bean.getAllThreadIds())) {
      switch (t.getThreadState()) {
        case BLOCKED:
        case WAITING:
        case TIMED_WAITING:
          lines.addAll(getThreadInfo(t, threadStacks.get(t.getThreadId())));
          if (t.getLockOwnerId() != -1) lockOwners.add(t.getLockOwnerId());
          break;
      }
    }

    if (lockOwners.size() > 0) {
      lines.add("\nLock Owners");
      for (ThreadInfo t : bean.getThreadInfo(Longs.toArray(lockOwners))) {
        lines.addAll(getThreadInfo(t, threadStacks.get(t.getThreadId())));
      }
    }

    return String.join("\n", lines);
  }

  private static List<String> getThreadInfo(ThreadInfo t, StackTraceElement[] stack) {
    List<String> lines = Lists.newLinkedList();

    lines.add(String.format("'%s' Id=%d %s",
        t.getThreadName(), t.getThreadId(), t.getThreadState()));
    lines.add("Waiting for lock: " + t.getLockName());
    lines.add("Lock is currently held by thread: " + t.getLockOwnerName());
    lines.add("Wait time: " + t.getBlockedTime() + " ms.");
    for (StackTraceElement s : stack) {
      lines.add(String.format("    " + s.toString()));
    }
    lines.add("\n");

    return lines;
  }
}
