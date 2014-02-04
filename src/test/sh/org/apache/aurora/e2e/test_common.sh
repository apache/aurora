# Common utility functions used by different variants of the
# end to end tests.

# Preserve original stderr so output from signal handlers doesn't get redirected to /dev/null.
exec 4>&2

_aurora_built=false
aurora() {
  if ! $_aurora_built
  then
    ./pants src/main/python/apache/aurora/client/bin:aurora_client
    _aurora_built=true
  fi
  ./dist/aurora_client.pex "$@"
}

_curl() { curl --silent --fail --retry 4 --retry-delay 10 "$@" ; }

collect_result() {
  (
    if [[ $RETCODE = 0 ]]
    then
      echo "***"
      echo "OK (all tests passed)"
      echo "***"
    else
      echo "!!!"
      echo "FAIL (something returned non-zero)"
      echo ""
      echo "This may be a transient failure (as in scheduler failover) or it could be a real issue"
      echo "with your code. Either way, this script DNR merging to master. Note you may need to"
      echo "reconcile state manually."
      echo "!!!"
      vagrant ssh aurora-scheduler -c "aurora kill example/vagrant/test/flask_example"
    fi
    exit $RETCODE
  ) >&4 # Send to the stderr we had at startup.
}
