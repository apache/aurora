python_library(
  name = 'hooks',
  sources = ['__init__.py', 'hooked_api.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/twitter/aurora/client:api'),
    pants('src/main/python/twitter/aurora/common:aurora_job_key'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
