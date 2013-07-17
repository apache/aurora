package com.twitter.aurora.scheduler.http;

import java.io.StringWriter;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.common.util.templating.StringTemplateHelper;
import com.twitter.common.util.templating.StringTemplateHelper.TemplateException;

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
