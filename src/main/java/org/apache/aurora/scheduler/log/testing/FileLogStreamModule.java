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
package org.apache.aurora.scheduler.log.testing;

import java.io.File;

import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.google.inject.PrivateModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;

import org.apache.aurora.scheduler.log.Log;

/**
 * Binding module that uses a local log file, intended for testing.
 */
public class FileLogStreamModule extends PrivateModule {

  // TODO(William Farner): Make this a required argument and ensure it is not included in production
  //                       builds (MESOS-471).
  //@NotNull
  @CmdLine(name = "testing_log_file_path", help = "Path to a file to store local log file data in.")
  private static final Arg<File> LOG_PATH = Arg.create(null);

  @Override
  protected void configure() {
    Preconditions.checkNotNull(LOG_PATH.get());
    bind(File.class).toInstance(LOG_PATH.get());
    bind(Log.class).to(FileLog.class);
    bind(FileLog.class).in(Singleton.class);
    expose(Log.class);
  }
}
