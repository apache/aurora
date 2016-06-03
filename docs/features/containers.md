Containers
==========

Docker
------

Aurora has optional support for launching Docker containers, if correctly [configured by an Operator](../operations/configuration.md#docker-containers).

Example (available in the [Vagrant environment](../getting-started/vagrant.md)):


    $ cat /vagrant/examples/jobs/docker/hello_docker.aurora
    hello_world_proc = Process(
      name = 'hello',
      cmdline = """
        while true; do
          echo hello world
          sleep 10
        done
      """)

    hello_world_docker = Task(
      name = 'hello docker',
      processes = [hello_world_proc],
      resources = Resources(cpu = 1, ram = 1*MB, disk=8*MB)
    )

    jobs = [
      Service(
        cluster = 'devcluster',
        environment = 'devel',
        role = 'docker-test',
        name = 'hello_docker',
        task = hello_world_docker,
        container = Container(docker = Docker(image = 'python:2.7'))
      )
    ]


In order to correctly execute processes inside a job, the docker container must have Python 2.7
installed. Further details of how to use Docker can be found in the
[Reference Documentation](../reference/configuration.md#docker-object).

Mesos
-----

*Note: In order to use filesystem images with Aurora, you must be running at least Mesos 0.28.x*

Aurora supports specifying a task filesystem image to use with the [Mesos containerizer](http://mesos.apache.org/documentation/latest/container-image/).
This is done by setting the ```container``` property of the Job to a ```Mesos``` container object
that includes the image to use. Both [AppC](https://github.com/appc/spec/blob/master/SPEC.md) and 
[Docker](https://github.com/docker/docker/blob/master/image/spec/v1.md) images are supported.

```
job = Job(
   ...
   container = Mesos(image=DockerImage(name='my-image', tag='my-tag'))
   ...
)
```
