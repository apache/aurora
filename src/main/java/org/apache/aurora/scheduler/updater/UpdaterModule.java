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
package org.apache.aurora.scheduler.updater;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;

/**
 * Binding module for scheduling logic and higher-level state management.
 */
public class UpdaterModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(JobUpdateController.class).to(JobUpdateControllerImpl.class);
    bind(JobUpdateControllerImpl.class).in(Singleton.class);
  }
}
