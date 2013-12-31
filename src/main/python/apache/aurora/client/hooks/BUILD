python_library(
  name = 'hooks',
  sources = ['__init__.py', 'hooked_api.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/apache/aurora/client:api'),
    pants('src/main/python/apache/aurora/common:aurora_job_key'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)
