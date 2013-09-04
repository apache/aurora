TEST_DATA = globs('*test.thrift')

java_thrift_library(
  name = 'thrift',
  provides = artifact(
    org = 'com.twitter.aurora',
    name = 'aurora',
    repo = pants('build-support/twitter/ivy:internal'),
  ),
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
