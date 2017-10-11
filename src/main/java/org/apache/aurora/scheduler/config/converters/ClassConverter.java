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

package org.apache.aurora.scheduler.config.converters;

import java.util.Map;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.BaseConverter;
import com.google.common.collect.ImmutableMap;

import org.apache.aurora.scheduler.http.api.security.IniShiroRealmModule;
import org.apache.aurora.scheduler.http.api.security.Kerberos5ShiroRealmModule;

public class ClassConverter extends BaseConverter<Class<?>> {

  private static final Map<String, String> NAME_ALIASES = ImmutableMap.of(
      "KERBEROS5_AUTHN", Kerberos5ShiroRealmModule.class.getCanonicalName(),
      "INI_AUTHNZ", IniShiroRealmModule.class.getCanonicalName());

  public ClassConverter(String optionName) {
    super(optionName);
  }

  @Override
  public Class<?> convert(String value) {
    if (value.isEmpty()) {
      throw new ParameterException(getErrorString(value, "must not be blank"));
    }

    String unaliased = NAME_ALIASES.getOrDefault(value, value);
    try {
      return Class.forName(unaliased);
    } catch (ClassNotFoundException e) {
      throw new ParameterException(e);
    }
  }
}
