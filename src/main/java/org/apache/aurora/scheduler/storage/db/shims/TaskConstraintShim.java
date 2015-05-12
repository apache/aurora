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
package org.apache.aurora.scheduler.storage.db.shims;

import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;

/**
 * An extension of {@link TaskConstraint} that does not throw {@link NullPointerException} when
 * accessors are called on unset fields.
 */
public class TaskConstraintShim extends TaskConstraint {
  @Override
  public ValueConstraint getValue() {
    if (isSet(_Fields.VALUE)) {
      return super.getValue();
    } else {
      return null;
    }
  }

  @Override
  public LimitConstraint getLimit() {
    if (isSet(_Fields.LIMIT)) {
      return super.getLimit();
    } else {
      return null;
    }
  }
}
