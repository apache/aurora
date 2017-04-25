Custom Executors
================

If the need arises to use a Mesos executor other than the Thermos executor, the scheduler can be
configured to utilize a custom executor by specifying the `-custom_executor_config` flag.
The flag must be set to the path of a valid executor configuration file.

The configuration file must be a valid **JSON array** and contain, at minimum,
one executor configuration including the name, command and resources fields and
must be pointed to by the `-custom_executor_config` flag when the scheduler is
started.

### Array Entry

Property                 | Description
-----------------------  | ---------------------------------
executor (required)      | Description of executor.
task_prefix (required) ) | Prefix given to tasks launched with this executor's configuration.
volume_mounts (optional) | Volumes to be mounted in container running executor.

#### executor

Property                 | Description
-----------------------  | ---------------------------------
name (required)          | Name of the executor.
command (required)       | How to run the executor.
resources (required)     | Overhead to use for each executor instance.

#### command

Property                 | Description
-----------------------  | ---------------------------------
value (required)         | The command to execute.
arguments (optional)     | A list of arguments to pass to the command.
uris (optional)          | List of resources to download into the task sandbox.
shell (optional)         | Run executor via shell.

A note on the command property (from [mesos.proto](https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto)):

```
1) If 'shell == true', the command will be launched via shell
   (i.e., /bin/sh -c 'value'). The 'value' specified will be
   treated as the shell command. The 'arguments' will be ignored.
2) If 'shell == false', the command will be launched by passing
   arguments to an executable. The 'value' specified will be
   treated as the filename of the executable. The 'arguments'
   will be treated as the arguments to the executable. This is
   similar to how POSIX exec families launch processes (i.e.,
   execlp(value, arguments(0), arguments(1), ...)).
```

##### uris (list)
* Follows the [Mesos Fetcher schema](http://mesos.apache.org/documentation/latest/fetcher/)

Property                 | Description
-----------------------  | ---------------------------------
value (required)         | Path to the resource needed in the sandbox.
executable (optional)    | Change resource to be executable via chmod.
extract (optional)       | Extract files from packed or compressed archives into the sandbox.
cache (optional)         | Use caching mechanism provided by Mesos for resources.

#### resources (list)

Property             | Description
-------------------  | ---------------------------------
name (required)      | Name of the resource: cpus or mem.
type (required)      | Type of resource. Should always be SCALAR.
scalar (required)    | Value in float for cpus or int for mem (in MBs)

### volume_mounts (list)

Property                     | Description
---------------------------  | ---------------------------------
host_path (required)         | Host path to mount inside the container.
container_path (required)    | Path inside the container where `host_path` will be mounted.
mode (required)              | Mode in which to mount the volume, Read-Write (RW) or Read-Only (RO).

A sample configuration is as follows:

```json
[
    {
      "executor": {
        "name": "myExecutor",
        "command": {
          "value": "myExecutor.a",
          "shell": "false",
          "arguments": [
            "localhost:2181",
            "-verbose",
            "-config myConfiguration.config"
          ],
          "uris": [
            {
              "value": "/dist/myExecutor.a",
              "executable": true,
              "extract": false,
              "cache": true
            },
            {
              "value": "/home/user/myConfiguration.config",
              "executable": false,
              "extract": false,
              "cache": false
            }
          ]
        },
        "resources": [
          {
            "name": "cpus",
            "type": "SCALAR",
            "scalar": {
              "value": 1.00
            }
          },
          {
            "name": "mem",
            "type": "SCALAR",
            "scalar": {
              "value": 512
            }
          }
        ]
      },
      "volume_mounts": [
        {
          "mode": "RO",
          "container_path": "/path/on/container",
          "host_path": "/path/to/host/directory"
        },
        {
          "mode": "RW",
          "container_path": "/container",
          "host_path": "/host"
        }
      ],
      "task_prefix": "my-executor-"
    }
]
```

It should be noted that if you do not use Thermos or a Thermos based executor, links in the scheduler's
Web UI for tasks will not work (at least for the time being).
Some information about launched tasks can still be accessed via the Mesos Web UI or via the Aurora Client.

### Using a custom executor

At this time, it is not currently possible create a job that runs on a custom executor using the default
Aurora client. To allow the scheduler to pick the correct executor, the `JobConfiguration.TaskConfig.ExecutorConfig.name`
field must be set to match the name used in the custom executor configuration blob. (e.g. to run a job using myExecutor,
`JobConfiguration.TaskConfig.ExecutorConfig.name` must be set to `myExecutor`). While support for modifying
this field in Pystachio created, the easiest way to launch jobs with custom executors is to use
an existing custom Client such as [gorealis](https://github.com/rdelval/gorealis).
