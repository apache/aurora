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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.google.inject.Module;
import com.twitter.common.args.ArgParser;
import com.twitter.common.args.parsers.NonParameterizedTypeParser;

/**
 * ArgParser for Guice modules. Constructs an instance of a Module with a given FQCN if it has a
 * public no-args constructor.
 */
@ArgParser
public class ModuleParser extends NonParameterizedTypeParser<Module> {
  @Override
  public Module doParse(String raw) throws IllegalArgumentException {
    Class<?> rawClass;
    try {
      rawClass = Class.forName(raw);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }

    if (!Module.class.isAssignableFrom(rawClass)) {
      throw new IllegalArgumentException(
          "Class " + raw + " must implement " + Module.class.getName());
    }
    @SuppressWarnings("unchecked")
    Class<? extends Module> moduleClass = (Class<? extends Module>) rawClass;

    Constructor<? extends Module> moduleConstructor;
    try {
      moduleConstructor = moduleClass.getConstructor();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          "Module " + raw + " must have a public no-args constructor.",
          e);
    }

    try {
      return moduleConstructor.newInstance();
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
