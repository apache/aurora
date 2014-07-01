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
package org.apache.aurora.scheduler.http;

import java.util.Objects;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import org.apache.aurora.gen.LockKey;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Servlet that exposes existing resource/operation locks.
 */
@Path("/locks")
public class Locks {

  private final LockManager lockManager;

  @Inject
  Locks(LockManager lockManager) {
    this.lockManager = Objects.requireNonNull(lockManager);
  }

  /**
   * Dumps existing locks.
   *
   * @return HTTP response.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLocks() {
    return Response.ok(Maps.transformValues(
        Maps.uniqueIndex(lockManager.getLocks(), TO_LOCK_KEY), TO_BEAN)).build();
  }

  private static final Function<ILock, String> TO_LOCK_KEY = new Function<ILock, String>() {
    @Override
    public String apply(ILock lock) {
      return lock.getKey().getSetField() == LockKey._Fields.JOB
          ? JobKeys.canonicalString(lock.getKey().getJob())
          : "Unknown lock key type: " + lock.getKey().getSetField();
    }
  };

  private static final Function<ILock, LockBean> TO_BEAN = new Function<ILock, LockBean>() {
    @Override
    public LockBean apply(ILock lock) {
      return new LockBean(lock);
    }
  };

  private static final class LockBean {
    private final ILock lock;

    LockBean(ILock lock) {
      this.lock = lock;
    }

    @JsonProperty("token")
    public String getToken() {
      return lock.getToken();
    }

    @JsonProperty("user")
    public String getUser() {
      return lock.getUser();
    }

    @JsonProperty("timestampMs")
    public long getTimestampMs() {
      return lock.getTimestampMs();
    }

    @JsonProperty("message")
    public String getMessage() {
      return lock.getMessage();
    }
  }
}
