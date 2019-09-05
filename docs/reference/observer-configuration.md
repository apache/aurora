# Observer Configuration Reference

The Aurora/Thermos observer can take a variety of configuration options through command-line arguments.
A list of the available options can be seen by running `thermos_observer --long-help`.

Please refer to the [Operator Configuration Guide](../operations/configuration.md) for details on how
to properly set the most important options.

```
$ thermos_observer.pex --long-help
Options:
  -h, --help, --short-help
                        show this help message and exit.
  --long-help           show options from all registered modules, not just the
                        __main__ module.
  --mesos-root=MESOS_ROOT
                        The mesos root directory to search for Thermos
                        executor sandboxes [default: /var/lib/mesos]
  --ip=IP               The IP address the observer will bind to. [default:
                        0.0.0.0]
  --port=PORT           The port on which the observer should listen.
                        [default: 1338]
  --polling_interval_secs=POLLING_INTERVAL_SECS
                        The number of seconds between observer refresh
                        attempts. [default: 5]
  --disable_task_resource_collection
                        Disable collection of CPU and memory statistics for
                        each active task. Those can be expensive to collect if
                        there are hundreds of active tasks per host. [default:
                        False]
  --task_process_collection_interval_secs=TASK_PROCESS_COLLECTION_INTERVAL_SECS
                        The number of seconds between per task process
                        resource collections. [default: 20]
  --task_disk_collection_interval_secs=TASK_DISK_COLLECTION_INTERVAL_SECS
                        The number of seconds between per task disk resource
                        collections. [default: 60]
  --enable_mesos_disk_collector
                        Delegate per task disk usage collection to agent.
                        Should be enabled in conjunction to disk isolation in
                        Mesos-agent. This is not compatible with an
                        authenticated agent API. [default: False]
  --agent_api_url=AGENT_API_URL
                        Mesos Agent API url. [default:
                        http://localhost:5051/containers]
  --executor_id_json_path=EXECUTOR_ID_JSON_PATH
                        `jmespath` to executor_id key in agent response json
                        object. [default: executor_id]
  --disk_usage_json_path=DISK_USAGE_JSON_PATH
                        `jmespath` to disk usage bytes value in agent response
                        json object. [default: statistics.disk_used_bytes]

  From module twitter.common.app:
    --app_daemonize     Daemonize this application. [default: False]
    --app_profile_output=FILENAME
                        Dump the profiling output to a binary profiling
                        format. [default: None]
    --app_daemon_stderr=TWITTER_COMMON_APP_DAEMON_STDERR
                        Direct this app's stderr to this file if daemonized.
                        [default: /dev/null]
    --app_debug         Print extra debugging information during application
                        initialization. [default: False]
    --app_rc_filename   Print the filename for the rc file and quit. [default:
                        False]
    --app_daemon_stdout=TWITTER_COMMON_APP_DAEMON_STDOUT
                        Direct this app's stdout to this file if daemonized.
                        [default: /dev/null]
    --app_profiling     Run profiler on the code while it runs.  Note this can
                        cause slowdowns. [default: False]
    --app_ignore_rc_file
                        Ignore default arguments from the rc file. [default:
                        False]
    --app_pidfile=TWITTER_COMMON_APP_PIDFILE
                        The pidfile to use if --app_daemonize is specified.
                        [default: None]

  From module twitter.common.log.options:
    --log_to_stdout=[scheme:]LEVEL
                        OBSOLETE - legacy flag, use --log_to_stderr instead.
                        [default: ERROR]
    --log_to_stderr=[scheme:]LEVEL
                        The level at which logging to stderr [default: ERROR].
                        Takes either LEVEL or scheme:LEVEL, where LEVEL is one
                        of ['INFO', 'NONE', 'WARN', 'ERROR', 'DEBUG', 'FATAL']
                        and scheme is one of ['google', 'plain'].
    --log_to_disk=[scheme:]LEVEL
                        The level at which logging to disk [default: INFO].
                        Takes either LEVEL or scheme:LEVEL, where LEVEL is one
                        of ['INFO', 'NONE', 'WARN', 'ERROR', 'DEBUG', 'FATAL']
                        and scheme is one of ['google', 'plain'].
    --log_dir=DIR       The directory into which log files will be generated
                        [default: /var/tmp].
    --log_simple        Write a single log file rather than one log file per
                        log level [default: False].
    --log_to_scribe=[scheme:]LEVEL
                        The level at which logging to scribe [default: NONE].
                        Takes either LEVEL or scheme:LEVEL, where LEVEL is one
                        of ['INFO', 'NONE', 'WARN', 'ERROR', 'DEBUG', 'FATAL']
                        and scheme is one of ['google', 'plain'].
    --scribe_category=CATEGORY
                        The category used when logging to the scribe daemon.
                        [default: python_default].
    --scribe_buffer     Buffer messages when scribe is unavailable rather than
                        dropping them. [default: False].
    --scribe_host=HOST  The host running the scribe daemon. [default:
                        localhost].
    --scribe_port=PORT  The port used to connect to the scribe daemon.
                        [default: 1463].
```
