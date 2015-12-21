Apache Aurora lets you use an [Apache Mesos](http://mesos.apache.org) cluster as a private cloud.
It supports running long-running services, cron jobs, and ad-hoc jobs.

Aurora aims to make it extremely quick and easy to take a built application and run it on machines
in a cluster, with an emphasis on reliability. It provides basic operations to manage services
running in a cluster, such as rolling upgrades.

To very concisely describe Aurora, it is a system that you can instruct to do things like
_run 100 of these, somewhere, forever_.

- [Features](#features)
	- [User-facing](#user-facing)
	- [Under the hood, to help you rest easy](#under-the-hood-to-help-you-rest-easy)
	- [When to use Aurora](#when-to-use-aurora)
	- [When to not use Aurora](#when-to-not-use-aurora)
	- [Companies using Aurora](#companies-using-aurora)
- [Getting Started](#getting-started)
- [Getting Help](#getting-help)
- [Requiremensts](#requiremensts)
- [How to build](#how-to-build)
	- [Testing](#testing)
	- [Compiling the source packages](#compiling-the-source-packages)
- [License](#license)

## Features

### User-facing
- Management of long-running services
- Cron scheduling
- Resource quotas: provide guaranteed resources for specific applications
- Rolling job updates, with automatic rollback
- Multi-user support
- Sophisticated [DSL](docs/configuration-tutorial.md): supports templating, allowing you to
  establish common patterns and avoid redundant configurations
- [Dedicated machines](docs/deploying-aurora-scheduler.md#dedicated-attribute):
  for things like stateful services that must always run on the same machines
- Service registration: [announce](docs/configuration-reference.md#announcer-objects) services in
  [ZooKeeper](http://zookeeper.apache.org/) for discovery by clients like
  [finagle](https://twitter.github.io/finagle).
- [Scheduling constraints](docs/configuration-reference.md#specifying-scheduling-constraints)
  to run on specific machines, or to mitigate impact of issues like machine and rack failure

### Under the hood, to help you rest easy
- Preemption: important services can 'steal' resources when they need it
- High-availability: resists machine failures and disk failures
- Scalable: proven to work in data center-sized clusters, with hundreds of users and thousands of
  jobs
- Instrumented: a wealth of information makes it easy to [monitor](docs/monitoring.md) and debug

### When to use Aurora
Aurora can take over for most uses of software like monit and chef.  Aurora can manage applications,
while these tools are still useful to manage Aurora and Mesos themselves.

### When to not use Aurora
If you have very specific scheduling requirements, or are building a system that looks like a
scheduler itself, you may want to explore developing your own
[framework](http://mesos.apache.org/documentation/latest/app-framework-development-guide).

### Companies using Aurora
- [Blue Yonder](http://www.blue-yonder.com)
- [Boxever](http://www.boxever.com)
- [Foursquare](https://foursquare.com)
- [Gutefrage.net](https://www.gutefrage.net)
- [Magine TV](https://magine.com)
- [Oscar Health](https://www.hioscar.com)
- [Sabre Labs](http://www.sabre.com)
- [TellApart](https://www.tellapart.com)
- [Twitter](https://twitter.com)

Are you using Aurora too?  Let us know, or submit a patch to join the list!

## Getting Started
* [Try the tutorial](docs/tutorial.md)
* [Running a Local Cluster with Vagrant](docs/vagrant.md)
* [Installing Aurora](docs/installing.md)
* [Developing Aurora](docs/developing-aurora-scheduler.md)

## Getting Help
If you have questions, you can reach out to our mailing list: dev@aurora.apache.org
([archive](http://mail-archives.apache.org/mod_mbox/aurora-dev)).
We're also often available in IRC: #aurora on
[irc.freenode.net](http://webchat.freenode.net/?channels=#aurora).

You can also file bugs/issues in our [JIRA queue](http://issues.apache.org/jira/browse/AURORA).

## Requirements
* Python 2.7 or higher
* JDK 1.8 or higher

* Source distribution requirements
       * [Gradle](http://gradle.org)

## How to build

Gradle and Bower are not shipped with the source distribution of Apache Aurora.
The following instructions apply for the source release downloads only. When
using Apache Aurora checked out from the source repository or the binary
distribution the Gradle wrapper and JavaScript dependencies are provided.

1. Install Gradle following the instructions on the [Gradle web site](http://gradle.org)
2. From the root directory of the Apache Aurora project generate the gradle
wrapper by running:
```shell
    gradle wrapper
```

### Testing
To run the same tests that are run in the Apache Aurora continuous integration
environment

* From the root directory of the Apache Aurora project run:
```shell
    ./build-support/jenkins/build.sh
```

* In addition, there is an end-to-end test that runs a suite of aurora commands
using a virtual cluster. To run the end-to-end tests:
```shell
    $ bash src/test/sh/org/apache/aurora/e2e/test_end_to_end.sh
```

### Compiling the source packages
To compile the source packages into binary distributions

* From the root directory of the Apache Aurora project run:
```shell
    ./gradlew distTar
    ./build-support/release/make-python-sdists
```

For additional information see the [Developing Aurora](docs/developing-aurora-scheduler.md)
guide.

## License
Except as otherwise noted this software is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
