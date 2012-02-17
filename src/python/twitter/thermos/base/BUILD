python_library(
  name = 'ckpt',
  sources = ['ckpt.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ]
)

python_library(
  name = 'path',
  sources = ['path.py'],
)

python_library(
  name = 'options',
  sources = ['options.py'],
  dependencies = [
    pants('src/thrift/com/twitter/thermos:py-thrift'),
    pants('3rdparty/python:pystachio')
  ]
)

python_library(
  name = 'base',
  dependencies = [
    pants(':ckpt'),
    pants(':path'),
    pants(':options'),
  ]
)
