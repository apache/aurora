/*
 * Copyright 2013 Twitter, Inc.
 *
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
package org.apache.aurora.scheduler.storage.mem;

import javax.annotation.Nullable;

import com.google.common.base.Function;

import org.apache.thrift.TBase;

/**
 * Utility class for common operations amongst in-memory store implementations.
 */
final class Util {

  private Util() {
    // Utility class.
  }

  /**
   * Creates a function that performs a 'deep copy' on a thrift struct of a specific type.  The
   * resulting copied objects will be exactly identical to the original.  Mutations to the original
   * object will not be reflected in the copy, and vice versa.
   *
   * @return A copier for the provided type of thrift structs.
   */
  static <T extends TBase<T, ?>> Function<T, T> deepCopier() {
    return new Function<T, T>() {
      @Override public T apply(@Nullable T input) {
        if (input == null) {
          return null;
        }

        @SuppressWarnings("unchecked")
        T t = (T) input.deepCopy();
        return t;
      }
    };
  }
}
