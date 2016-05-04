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
package org.apache.aurora.scheduler.resources;

import org.apache.mesos.Protos.Resource;

/**
 * Converts Mesos resource values to be consumed in Aurora.
 */
public interface MesosResourceConverter {

  /**
   * Gets Mesos resource quantity.
   *
   * @param resource Mesos resource to quantify.
   * @return Mesos resource quantity.
   */
  Double quantify(Resource resource);

  ScalarConverter SCALAR = new ScalarConverter();
  RangeConverter RANGES = new RangeConverter();

  class ScalarConverter implements MesosResourceConverter {
    @Override
    public Double quantify(Resource resource) {
      return resource.getScalar().getValue();
    }
  }

  class RangeConverter implements MesosResourceConverter {
    @Override
    public Double quantify(Resource resource) {
      return resource.getRanges().getRangeList().stream()
          .map(range -> 1 + range.getEnd() - range.getBegin())
          .reduce((l, r) -> l + r)
          .map(v -> v.doubleValue())
          .orElse(0.0);
    }
  }
}
