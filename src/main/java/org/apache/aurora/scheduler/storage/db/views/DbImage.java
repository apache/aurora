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
package org.apache.aurora.scheduler.storage.db.views;

import org.apache.aurora.gen.AppcImage;
import org.apache.aurora.gen.DockerImage;
import org.apache.aurora.gen.Image;

public final class DbImage {
  private AppcImage appc;
  private DockerImage docker;

  private DbImage() {
  }

  Image toThrift() {
    if (appc != null) {
      return Image.appc(appc);
    }

    if (docker != null) {
      return Image.docker(docker);
    }

    throw new IllegalStateException("Unknown image type.");
  }
}
