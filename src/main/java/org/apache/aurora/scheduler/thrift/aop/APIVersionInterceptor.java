package com.twitter.aurora.scheduler.thrift.aop;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import com.twitter.aurora.gen.Response;

import static com.twitter.aurora.gen.apiConstants.CURRENT_API_VERSION;

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
