python_library(
  name = 'hooks',
  sources = ['__init__.py', 'hooked_api.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/aurora/client:api'),
    pants('src/python/twitter/aurora/common:aurora_job_key'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
