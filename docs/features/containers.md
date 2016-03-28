Containers
==========


Docker
------

Aurora has optional support for launching Docker containers, if correctly [configured by an Operator](../operations/configuration.md#docker-containers).

Example (available in the [Vagrant environment](../getting-started/vagrant.md)):


    $ cat /vagrant/examples/jobs/docker/hello_docker.aurora
    hello_docker = Process(
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
[Reference Documentation](..reference/configuration.md#docker-object).
