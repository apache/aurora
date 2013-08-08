TEST_DATA = globs('*test.thrift')

java_thrift_library(
  name = 'thrift',
  sources = globs('*.thrift') - TEST_DATA,
)

java_thrift_library(
  name = 'thrift-test',
  sources = TEST_DATA,
)

python_thrift_library(
  name = 'py-thrift',
  sources = [
    'api.thrift',
    'internal_rpc.thrift',
  ],
)

python_thrift_library(
  name = 'py-thrift-test',
  sources = TEST_DATA,
)

python_thrift_library(
  name = 'py-thrift-storage',
  sources = ['storage.thrift'],
)
