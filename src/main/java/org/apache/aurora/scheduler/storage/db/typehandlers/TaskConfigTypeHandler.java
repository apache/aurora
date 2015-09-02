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
package org.apache.aurora.scheduler.storage.db.typehandlers;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.TaskConfig;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

/**
 * Type handler for objects of type {@link TaskConfig}. Converts {@link TaskConfig} to/from byte
 * array to be stored in SQL as BINARY type.
 *
 * <p/>
 * NOTE: We don't want to store serialized thrift objects long-term, but instead plan to reference
 * a canonical table of task configurations. This class will go away with AURORA-647.
 */
class TaskConfigTypeHandler implements TypeHandler<TaskConfig> {

  @Override
  public final void setParameter(
      PreparedStatement ps,
      int i,
      TaskConfig parameter,
      JdbcType jdbcType) throws SQLException {

    try {
      ps.setBytes(i, ThriftBinaryCodec.encodeNonNull(parameter));
    } catch (CodingException e) {
      throw new SQLException("Failed to encode thrift struct.", e);
    }
  }

  @Override
  public final TaskConfig getResult(ResultSet rs, String columnName) throws SQLException {
    return decodeOrThrow(rs.getBytes(columnName));
  }

  @Override
  public final TaskConfig getResult(ResultSet rs, int columnIndex) throws SQLException {
    return decodeOrThrow(rs.getBytes(columnIndex));
  }

  @Override
  public final TaskConfig getResult(CallableStatement cs, int columnIndex) throws SQLException {
    return decodeOrThrow(cs.getBytes(columnIndex));
  }

  private TaskConfig decodeOrThrow(byte[] value) throws SQLException {
    try {
      return ThriftBinaryCodec.decode(TaskConfig.class, value);
    } catch (CodingException e) {
      throw new SQLException("Failed to decode thrift struct.", e);
    }
  }
}
