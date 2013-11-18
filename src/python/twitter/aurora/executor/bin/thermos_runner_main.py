from twitter.common import app
from twitter.common.log.options import LogOptions
from twitter.aurora.executor.thermos_runner import proxy_main as runner_proxy_main


LogOptions.set_simple(True)


def proxy_main():
  main = runner_proxy_main

  app.main()
