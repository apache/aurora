TEST_DATA = globs('*_test.thrift')

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
    'mesos_comm.thrift',
    'mesos_twitter.thrift',
  ],
)

python_thrift_library(
  name = 'py-thrift-test',
  sources = TEST_DATA,
)

python_thrift_library(
  name = 'py-thrift-storage',
  sources = ['mesos_storage.thrift'],
)
