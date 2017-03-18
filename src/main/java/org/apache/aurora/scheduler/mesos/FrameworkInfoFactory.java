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
package org.apache.aurora.scheduler.mesos;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.MalformedURLException;
import java.net.URL;
import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;

import org.apache.aurora.scheduler.http.HttpService;
import org.apache.mesos.v1.Protos.FrameworkInfo;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public interface FrameworkInfoFactory {
  /**
   * Creates the FrameworkInfo for Mesos.
   */
  FrameworkInfo getFrameworkInfo();

  class FrameworkInfoFactoryImpl implements FrameworkInfoFactory {
    /**
     * Binding annotation for the base FrameworkInfo.
     */
    @VisibleForTesting
    @Qualifier
    @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
    public @interface BaseFrameworkInfo { }

    /**
     * Annotation for the protocol to advertise.
     */
    @Qualifier
    @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
    public @interface SchedulerProtocol { }

    private final FrameworkInfo baseInfo;
    private final HttpService service;
    private final String protocol;

    @Inject
    public FrameworkInfoFactoryImpl(
        @BaseFrameworkInfo FrameworkInfo base,
        HttpService service,
        @SchedulerProtocol String protocol) {

      this.baseInfo = requireNonNull(base);
      this.service = requireNonNull(service);
      this.protocol = requireNonNull(protocol);
    }

    @Override
    public FrameworkInfo getFrameworkInfo() {
      HostAndPort hostAndPort = service.getAddress();
      URL url;
      try {
        url = new URL(protocol, hostAndPort.getHost(), hostAndPort.getPort(), "");
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }

      FrameworkInfo.Builder info = baseInfo.toBuilder();

      info.setHostname(hostAndPort.getHost());
      info.setWebuiUrl(url.toString());

      return info.build();
    }
  }
}
