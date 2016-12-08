Long-running Services
=====================

Jobs that are always restart on completion, whether successful or unsuccessful,
are called services. This is useful for long-running processes
such as webservices that should always be running, unless stopped explicitly.


Service Specification
---------------------

A job is identified as a service by the presence of the flag
``service=True` in the [`Job`](../reference/configuration.md#job-objects) object.
The `Service` alias can be used as shorthand for `Job` with `service=True`.

Example (available in the [Vagrant environment](../getting-started/vagrant.md)):

    $ cat /vagrant/examples/jobs/hello_world.aurora
    hello = Process(
      name = 'hello',
      cmdline = """
        while true; do
          echo hello world
          sleep 10
        done
      """)

    task = SequentialTask(
      processes = [hello],
      resources = Resources(cpu = 1.0, ram = 128*MB, disk = 128*MB)
    )

    jobs = [
      Service(
        task = task,
        cluster = 'devcluster',
        role = 'www-data',
        environment = 'prod',
        name = 'hello'
      )
    ]


Jobs without the service bit set only restart up to `max_task_failures` times and only if they
terminated unsuccessfully either due to human error or machine failure (see the
[`Job`](../reference/configuration.md#job-objects) object for details).


Ports
-----

In order to be useful, most services have to bind to one or more ports. Aurora enables this
usecase via the [`thermos.ports` namespace](../reference/configuration.md#thermos-namespace) that
allows to request arbitrarily named ports:


    nginx = Process(
      name = 'nginx',
      cmdline = './run_nginx.sh -port {{thermos.ports[http]}}'
    )


When this process is included in a job, the job will be allocated a port, and the command line
will be replaced with something like:

    ./run_nginx.sh -port 42816

Where 42816 happens to be the allocated port.

For details on how to enable clients to discover this dynamically assigned port, see our
[Service Discovery](service-discovery.md) documentation.


Health Checking
---------------

Typically, the Thermos executor monitors processes within a task only by liveness of the forked
process. In addition to that, Aurora has support for rudimentary health checking: Either via HTTP
via custom shell scripts.

For example, simply by requesting a `health` port, a process can request to be health checked
via repeated calls to the `/health` endpoint:

    nginx = Process(
      name = 'nginx',
      cmdline = './run_nginx.sh -port {{thermos.ports[health]}}'
    )

Please see the
[configuration reference](../reference/configuration.md#user-content-healthcheckconfig-objects)
for configuration options for this feature.

Starting with the 0.17.0 release, job updates rely only on task health-checks by introducing
a `min_consecutive_successes` parameter on the HealthCheckConfig object. This parameter represents
the number of successful health checks needed before a task is moved into the `RUNNING` state. Tasks
that do not have enough successful health checks within the first `n` attempts, are moved to the
`FAILED` state, where `n = ceil(initial_interval_secs/interval_secs) + max_consecutive_failures +
min_consecutive_successes`. In order to accommodate variability during task warm up, `initial_interval_secs`
will act as a grace period. Any health-check failures during the first `m` attempts are ignored and
do not count towards `max_consecutive_failures`, where `m = ceil(initial_interval_secs/interval_secs)`.

As [job updates](job-updates.md) are based only on health-checks, it is not necessary to set
`watch_secs` to the worst-case update time, it can instead be set to 0. The scheduler considers a
task that is in the `RUNNING` to be healthy and proceeds to updating the next batch of instances.
For details on how to control health checks, please see the
[HealthCheckConfig](../reference/configuration.md#healthcheckconfig-objects) configuration object.
Existing jobs that do not configure a health-check can fall-back to using `watch_secs` to
monitor a task before considering it healthy.

You can pause health checking by touching a file inside of your sandbox, named `.healthchecksnooze`.
As long as that file is present, health checks will be disabled, enabling users to gather core
dumps or other performance measurements without worrying about Aurora's health check killing
their process.

WARNING: Remember to remove this when you are done, otherwise your instance will have permanently
disabled health checks.
