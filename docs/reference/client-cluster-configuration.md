# Client Cluster Configuration

A cluster configuration file is used by the Aurora client to describe the Aurora clusters with
which it can communicate. Ultimately this allows client users to reference clusters with short names
like us-east and eu.

A cluster configuration is formatted as JSON.  The simplest cluster configuration is one that
communicates with a single (non-leader-elected) scheduler.  For example:

    [{
      "name": "example",
      "scheduler_uri": "http://localhost:55555",
    }]


A configuration for a leader-elected scheduler would contain something like:

    [{
      "name": "example",
      "zk": "192.168.33.7",
      "scheduler_zk_path": "/aurora/scheduler"
    }]


The following properties may be set:

  **Property**             | **Type** | **Description**
  :------------------------| :------- | :--------------
   **name**                | String   | Cluster name (Required)
   **slave_root**          | String   | Path to mesos slave work dir (Required)
   **slave_run_directory** | String   | Name of mesos slave run dir (Required)
   **zk**                  | String   | Hostname of ZooKeeper instance used to resolve Aurora schedulers.
   **zk_port**             | Integer  | Port of ZooKeeper instance used to locate Aurora schedulers (Default: 2181)
   **scheduler_zk_path**   | String   | ZooKeeper path under which scheduler instances are registered.
   **scheduler_uri**       | String   | URI of Aurora scheduler instance.
   **proxy_url**           | String   | Used by the client to format URLs for display.
   **auth_mechanism**      | String   | The authentication mechanism to use when communicating with the scheduler. (Default: UNAUTHENTICATED)


## Details

### `name`

The name of the Aurora cluster represented by this entry. This name will be the `cluster` portion of
any job keys identifying jobs running within the cluster.

### `slave_root`

The path on the mesos slaves where executing tasks can be found. It is used in combination with the
`slave_run_directory` property by `aurora task run` and `aurora task ssh` to change into the sandbox
directory after connecting to the host. This value should match the value passed to `mesos-slave`
as `-work_dir`.

### `slave_run_directory`

The name of the directory where the task run can be found. This is used in combination with the
`slave_root` property by `aurora task run` and `aurora task ssh` to change into the sandbox
directory after connecting to the host. This should almost always be set to `latest`.

### `zk`

The hostname of the ZooKeeper instance used to resolve the Aurora scheduler. Aurora uses ZooKeeper
to elect a leader. The client will connect to this ZooKeeper instance to determine the current
leader. This host should match the host passed to the scheduler as `-zk_endpoints`.

### `zk_port`

The port on which the ZooKeeper instance is running. If not set this will default to the standard
ZooKeeper port of 2181. This port should match the port in the host passed to the scheduler as
`-zk_endpoints`.

### `scheduler_zk_path`

The path on the ZooKeeper instance under which the Aurora serverset is registered. This value should
match the value passed to the scheduler as `-serverset_path`.

### `scheduler_uri`

The URI of the scheduler. This would be used in place of the ZooKeeper related configuration above
in circumstances where direct communication with a single scheduler is needed (e.g. testing
environments). It is strongly advised to **never** use this property for production deploys.

### `proxy_url`

Instead of using the hostname of the leading scheduler as the base url, if `proxy_url` is set, its
value will be used instead. In that scenario the value for `proxy_url` would be, for example, the
URL of your VIP in a loadbalancer or a roundrobin DNS name.

### `auth_mechanism`

The identifier of an authentication mechanism that the client should use when communicating with the
scheduler. Support for values other than `UNAUTHENTICATED` requires a matching scheduler-side
[security configuration](../operations/security.md).
