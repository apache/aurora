python_binary(
  name = 'thermos_observer',
  source = 'thermos_observer.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:http'),
    pants('src/python/twitter/thermos:cherrypy'),
    pants('src/python/twitter/thermos/common:path'),
    pants('src/python/twitter/thermos/observer/http:http_observer'),
    pants('src/python/twitter/thermos/observer:task_observer'),
  ],
)
