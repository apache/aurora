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
package org.apache.aurora.scheduler.storage.db;

/**
 * MyBatis returns auto-generated IDs through mutable fields in parameters.
 * This class can be used as an additional {@link org.apache.ibatis.annotations.Param Param} to
 * retrieve the ID when the inserted object is not self-identifying.
 */
class InsertResult {
  private long id = Long.MIN_VALUE;
  private boolean isSet;

  long getId() {
    if (!isSet) {
      throw new IllegalStateException("Missing ID value.");
    }
    return id;
  }

  void setId(long value) {
    id = value;
    isSet = true;
  }
}
