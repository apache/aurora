package org.apache.aurora.scheduler.thrift.aop;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import org.apache.aurora.gen.Response;

import static org.apache.aurora.gen.apiConstants.CURRENT_API_VERSION;

class APIVersionInterceptor implements MethodInterceptor {

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    Response resp = (Response) invocation.proceed();
    if (resp.version == null) {
      resp.setVersion(CURRENT_API_VERSION);
    }
    return resp;
  }
}
