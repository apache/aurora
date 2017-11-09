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
package org.apache.aurora.scheduler.storage.log;

import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import org.apache.aurora.scheduler.log.Log;

public class FakeLog implements Log {
  private final NavigableMap<Long, byte[]> data = Maps.newTreeMap();

  @Override
  public Stream open() throws IOException {
    return new FakeStream();
  }

  private static class LongPosition implements Position {
    private final long position;

    LongPosition(long position) {
      this.position = position;
    }
  }

  private class FakeStream implements Stream {
    @Override
    public Position append(byte[] contents) throws StreamAccessException {
      long position = data.isEmpty() ? 1 : data.lastKey() + 1;
      data.put(position, contents);
      return new LongPosition(position);
    }

    @Override
    public Iterator<Entry> readAll() throws StreamAccessException {
      return Iterators.transform(data.values().iterator(), e -> new Entry() {
        @Override
        public byte[] contents() {
          return e;
        }
      });
    }

    @Override
    public void truncateBefore(Position position) {
      if (!(position instanceof LongPosition)) {
        throw new IllegalArgumentException("Wrong position type");
      }

      data.headMap(((LongPosition) position).position).clear();
    }
  }
}
