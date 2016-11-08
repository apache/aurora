Containers
==========

Aurora supports several containerizers, notably the Mesos containerizer and the Docker
containerizer. The Mesos containerizer uses native OS features directly to provide isolation between
containers, while the Docker containerizer delegates container management to the Docker engine.

The support for launching container images via both containerizers has to be
[enabled by a cluster operator](../operations/configuration.md#containers).

Mesos Containerizer
-------------------

The Mesos containerizer is the native Mesos containerizer solution. It allows tasks to be
run with an array of [pluggable isolators](resource-isolation.md) and can launch tasks using
[Docker](https://github.com/docker/docker/blob/master/image/spec/v1.md) images,
[AppC](https://github.com/appc/spec/blob/master/SPEC.md) images, or directly on the agent host
filesystem.

The following example (available in our [Vagrant environment](../getting-started/vagrant.md))
launches a hello world example within a `debian/jessie` Docker image:

    $ cat /vagrant/examples/jobs/hello_docker_image.aurora
    hello_loop = Process(
      name = 'hello',
      cmdline = """
        while true; do
          echo hello world
          sleep 10
        done
      """)

    task = Task(
      processes = [hello_loop],
      resources = Resources(cpu=1, ram=1*MB, disk=8*MB)
    )

    jobs = [
      Service(
        cluster = 'devcluster',
        environment = 'devel',
        role = 'www-data',
        name = 'hello_docker_image',
        task = task,
        container = Mesos(image=DockerImage(name='debian', tag='jessie'))
      )
    ]

Docker and Appc images are designated using an appropriate `image` property of the `Mesos`
configuration object. If either `container` or `image` is left unspecified, the host filesystem
will be used. Further details of how to specify images can be found in the
[Reference Documentation](../reference/configuration.md#mesos-object).

By default, Aurora launches processes as the Linux user named like the used role (e.g. `www-data`
in the example above). This user has to exist on the host filesystem. If it does not exist within
the container image, it will be created automatically. Otherwise, this user and its primary group
has to exist in the image with matching uid/gid.

For more information on the Mesos containerizer filesystem, namespace, and isolator features, visit
[Mesos Containerizer](http://mesos.apache.org/documentation/latest/mesos-containerizer/) and
[Mesos Container Images](http://mesos.apache.org/documentation/latest/container-image/).


Docker Containerizer
--------------------

The Docker containerizer launches container images using the Docker engine. It may often provide
more advanced features than the native Mesos containerizer, but has to be installed separately to
Mesos on each agent host.

Starting with the 0.17.0 release, `image` can be specified with a `{{docker.image[name][tag]}}` binder so that
the tag can be resolved to a concrete image digest. This ensures that the job always uses the same image
across restarts, even if the version identified by the tag has been updated, guaranteeing that only job
updates can mutate configuration.

Example (available in the [Vagrant environment](../getting-started/vagrant.md)):

    $ cat /vagrant/examples/jobs/hello_docker_engine.aurora
    hello_loop = Process(
      name = 'hello',
      cmdline = """
        while true; do
          echo hello world
          sleep 10
        done
      """)

    task = Task(
      processes = [hello_loop],
      resources = Resources(cpu=1, ram=1*MB, disk=8*MB)
    )

    jobs = [
      Service(
        cluster = 'devcluster',
        environment = 'devel',
        role = 'www-data',
        name = 'hello_docker',
        task = task,
        container = Docker(image = 'python:2.7')
      ), Service(
        cluster = 'devcluster',
        environment = 'devel',
        role = 'www-data',
        name = 'hello_docker_engine_binding',
        task = task,
        container = Docker(image = '{{docker.image[library/python][2.7]}}')
      )
    ]

Note, this feature requires a v2 Docker registry. If using a private Docker registry its url
must be specified in the `clusters.json` configuration file under the key `docker_registry`.
If not specified `docker_registry` defaults to `https://registry-1.docker.io` (Docker Hub).

Example:
    # clusters.json
    [{
      "name": "devcluster",
      ...
      "docker_registry": "https://registry.example.com"
    }]

Details of how to use Docker via the Docker engine can be found in the
[Reference Documentation](../reference/configuration.md#docker-object). Please note that in order to
correctly execute processes inside a job, the Docker container must have Python 2.7 and potentitally
further Mesos dependencies installed. This limitation does not hold for Docker containers used via
the Mesos containerizer.

For more information on launching Docker containers through the Docker containerizer, visit
[Docker Containerizer](http://mesos.apache.org/documentation/latest/docker-containerizer/)
