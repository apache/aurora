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
package org.apache.aurora.scheduler.http;

import java.io.StringWriter;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.twitter.common.base.Closure;
import com.twitter.common.util.templating.StringTemplateHelper;
import com.twitter.common.util.templating.StringTemplateHelper.TemplateException;

import org.antlr.stringtemplate.StringTemplate;

/**
 * Base class for common functions needed in a jersey stringtemplate servlet.
 */
abstract class JerseyTemplateServlet {

  private final StringTemplateHelper templateHelper;

  JerseyTemplateServlet(String templatePath) {
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
