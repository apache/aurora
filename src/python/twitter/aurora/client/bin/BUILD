python_binary(
  name = 'aurora_client',
  source = 'aurora_client.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/aurora/client:base'),
    pants('src/python/twitter/aurora/client/commands:all'),
  ]
)

python_binary(
  name = 'aurora_admin',
  source = 'aurora_admin.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/aurora/client:base'),
    pants('src/python/twitter/aurora/client:options'),
    pants('src/python/twitter/aurora/client/commands:admin'),
    pants('src/python/twitter/aurora/client/commands:help'),
  ]
)
