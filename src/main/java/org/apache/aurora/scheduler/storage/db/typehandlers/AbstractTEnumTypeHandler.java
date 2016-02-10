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

import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.thrift.TEnum;

import static com.google.common.base.Preconditions.checkState;

/**
 * Type handler for fields of type {@link TEnum}.  Implementers need only override
 * {@link #fromValue(int)}.
 *
 * @param <T> Enum type.
 */
abstract class AbstractTEnumTypeHandler<T extends TEnum> implements TypeHandler<T> {

  /**
   * Finds the enum value associated with the provided integer identity.
   *
   * @param value Value to find in the enum values.
   * @return Enum value associated with {@code value}.
   */
  protected abstract T fromValue(int value);

  @Override
  public final void setParameter(PreparedStatement ps, int i, T parameter, JdbcType jdbcType)
      throws SQLException {

    ps.setInt(i, parameter.getValue());
  }

  @Override
  public final T getResult(ResultSet rs, String columnName) throws SQLException {
    int i = rs.getInt(columnName);
    checkState(!rs.wasNull());
    return fromValue(i);
  }

  @Override
  public final T getResult(ResultSet rs, int columnIndex) throws SQLException {
    int i = rs.getInt(columnIndex);
    checkState(!rs.wasNull());
    return fromValue(i);
  }

  @Override
  public final T getResult(CallableStatement cs, int columnIndex) throws SQLException {
    int i = cs.getInt(columnIndex);
    checkState(!cs.wasNull());
    return fromValue(i);
  }
}
