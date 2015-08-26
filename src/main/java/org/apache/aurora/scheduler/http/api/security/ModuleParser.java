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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;

import org.apache.aurora.common.args.ArgParser;
import org.apache.aurora.common.args.parsers.NonParameterizedTypeParser;
import org.apache.aurora.scheduler.app.Modules;

/**
 * ArgParser for Guice modules. Constructs an instance of a Module with a given alias or FQCN if it
 * has a public no-args constructor.
 */
@ArgParser
public class ModuleParser extends NonParameterizedTypeParser<Module> {
  private static final Map<String, String> NAME_ALIASES = ImmutableMap.of(
      "KERBEROS5_AUTHN", Kerberos5ShiroRealmModule.class.getCanonicalName(),
      "INI_AUTHNZ", IniShiroRealmModule.class.getCanonicalName());

  @Override
  public Module doParse(String raw) throws IllegalArgumentException {
    String fullyQualifiedName = NAME_ALIASES.containsKey(raw) ? NAME_ALIASES.get(raw) : raw;
    Class<?> rawClass;
    try {
      rawClass = Class.forName(fullyQualifiedName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }

    if (!Module.class.isAssignableFrom(rawClass)) {
      throw new IllegalArgumentException(
          "Class " + fullyQualifiedName + " must implement " + Module.class.getName());
    }
    @SuppressWarnings("unchecked")
    Class<? extends Module> moduleClass = (Class<? extends Module>) rawClass;

    return Modules.lazilyInstantiated(moduleClass);
  }
}
