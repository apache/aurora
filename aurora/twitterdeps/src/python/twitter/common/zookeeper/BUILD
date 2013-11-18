python_requirement(
  name = 'zookeeper',
  requirement = 'twitter.common.zookeeper==0.1.1',
)

python_library(
  name = 'kazoo_client',
  dependencies = [
    pants(':zookeeper'),
  ],
)
