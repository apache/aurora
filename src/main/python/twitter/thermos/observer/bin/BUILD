python_binary(
  name = 'thermos_observer',
  source = 'thermos_observer.py',
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    # XXX(wickman) The Thermos observer is broken without the http app module.
    # pants('src/python/twitter/common/app/modules:http'),
    pants('src/main/python/twitter/thermos:cherrypy'),
    pants('src/main/python/twitter/thermos/common:path'),
    pants('src/main/python/twitter/thermos/observer/http:http_observer'),
    pants('src/main/python/twitter/thermos/observer:task_observer'),
  ],
)
