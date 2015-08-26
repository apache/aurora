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
package org.apache.aurora.scheduler.http;

import java.io.StringWriter;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.aurora.common.base.Closure;
import org.apache.aurora.common.util.templating.StringTemplateHelper;
import org.apache.aurora.common.util.templating.StringTemplateHelper.TemplateException;

/**
 * Base class for common functions needed in a jersey stringtemplate servlet.
 *
 * TODO(wfarner): This class should be composed rather than extended.  Turn it into a helper class.
 */
class JerseyTemplateServlet {

  private final StringTemplateHelper templateHelper;

  protected JerseyTemplateServlet(String templatePath) {
    templateHelper = new StringTemplateHelper(getClass(), templatePath, true);
  }

  protected final Response fillTemplate(Closure<StringTemplate> populator) {
    StringWriter output = new StringWriter();
    try {
      templateHelper.writeTemplate(output, populator);
    } catch (TemplateException e) {
      throw new WebApplicationException(e);
    }
    return Response.ok(output.toString()).build();
  }
}
