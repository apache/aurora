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
package org.apache.aurora.scheduler.http.api.security;

import com.google.common.base.Optional;

import org.apache.aurora.scheduler.http.api.security.FieldGetter.AbstractFieldGetter;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.StructMetaData;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Retrieves an optional struct-type field from a struct.
 */
class ThriftFieldGetter<T extends TBase<T, F>, F extends TFieldIdEnum, V extends TBase<V, ?>>
    extends AbstractFieldGetter<T, V> {

  private final F fieldId;

  ThriftFieldGetter(Class<T> structClass, F fieldId, Class<V> valueClass) {
    super(structClass, valueClass);

    FieldValueMetaData fieldValueMetaData = FieldMetaData
        .getStructMetaDataMap(structClass)
        .get(fieldId)
        .valueMetaData;

    checkArgument(fieldValueMetaData instanceof StructMetaData);
    StructMetaData structMetaData = (StructMetaData) fieldValueMetaData;
    checkArgument(
        valueClass.equals(structMetaData.structClass),
        "Value class %s does not match field metadata for %s (expected %s)",
        valueClass.getName(),
        fieldId,
        structMetaData.structClass);

    this.fieldId = fieldId;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<V> apply(T input) {
    if (input.isSet(fieldId)) {
      return Optional.of((V) input.getFieldValue(fieldId));
    } else {
      return Optional.absent();
    }
  }
}
