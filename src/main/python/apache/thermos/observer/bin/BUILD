python_binary(
  name = 'thermos_observer',
  source = 'thermos_observer.py',
  entry_point = 'twitter.thermos.observer.bin.thermos_observer:proxy_main',
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/http'),
    pants('src/main/python/twitter/thermos:cherrypy'),
    pants('src/main/python/twitter/thermos/common:path'),
    pants('src/main/python/twitter/thermos/observer/http:http_observer'),
    pants('src/main/python/twitter/thermos/observer:task_observer'),
  ],
)
