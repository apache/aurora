![Aurora Logo](docs/images/aurora_logo.png)

[Apache Aurora](https://aurora.apache.org/) lets you use an [Apache Mesos](http://mesos.apache.org)
cluster as a private cloud. It supports running long-running services, cron jobs, and ad-hoc jobs.
Aurora aims to make it extremely quick and easy to take a built application and run it on machines
in a cluster, with an emphasis on reliability. It provides basic operations to manage services
running in a cluster, such as rolling upgrades.

To very concisely describe Aurora, it is like a distributed monit or distributed supervisord that
you can instruct to do things like _run 100 of these, somewhere, forever_.


## Features

Aurora is built for users _and_ operators.

* User-facing Features:
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

* Under the hood, to help you rest easy:
  - Preemption: important services can 'steal' resources when they need it
  - High-availability: resists machine failures and disk failures
  - Scalable: proven to work in data center-sized clusters, with hundreds of users and thousands of
    jobs
  - Instrumented: a wealth of information makes it easy to [monitor](docs/monitoring.md) and debug

### When and when not to use Aurora
Aurora can take over for most uses of software like monit and chef.  Aurora can manage applications,
while these tools are still useful to manage Aurora and Mesos themselves.

However, if you have very specific scheduling requirements, or are building a system that looks like a
scheduler itself, you may want to explore developing your own
[framework](http://mesos.apache.org/documentation/latest/app-framework-development-guide).

### Companies using Aurora
Are you using Aurora too?  Let us know, or submit a patch to join the list!

- [Blue Yonder](http://www.blue-yonder.com)
- [Boxever](http://www.boxever.com)
- [Foursquare](https://foursquare.com)
- [Gutefrage.net](https://www.gutefrage.net)
- [Magine TV](https://magine.com)
- [Medallia](http://www.medallia.com)
- [Oscar Health](https://www.hioscar.com)
- [Sabre Labs](http://www.sabre.com)
- [TellApart](https://www.tellapart.com)
- [Twitter](https://twitter.com)


## Getting Help
If you have questions that aren't answered in our [doucmentation](https://aurora.apache.org/documentation/latest/), you can reach out to one of our [mailing lists](https://aurora.apache.org/community/). We're also often available in IRC: #aurora on
[irc.freenode.net](http://webchat.freenode.net/?channels=#aurora).

You can also file bugs/issues in our [JIRA queue](http://issues.apache.org/jira/browse/AURORA).


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
