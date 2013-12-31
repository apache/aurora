python_binary(
  name = 'thermos_observer',
  source = 'thermos_observer.py',
  entry_point = 'apache.thermos.observer.bin.thermos_observer:proxy_main',
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/http'),
    pants('src/main/python/apache/thermos:cherrypy'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/python/apache/thermos/observer/http:http_observer'),
    pants('src/main/python/apache/thermos/observer:task_observer'),
  ],
)
