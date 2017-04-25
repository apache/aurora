Webhooks
========

Aurora has an optional feature which allows operator to specify a file to configure a HTTP webhook
to receive task state change events. It can be enabled with a scheduler flag eg
`-webhook_config=/path/to/webhook.json`. At this point, webhooks are still considered *experimental*.

Below is a sample configuration:

```json
{
  "headers": {
    "Content-Type": "application/vnd.kafka.json.v1+json",
    "Producer-Type": "reliable"
  },
  "targetURL": "http://localhost:5000/",
  "timeoutMsec": 5
}
```

And an example of a response that you will get back:

```json
{
    "task":
    {
        "cachedHashCode":0,
        "assignedTask": {
            "cachedHashCode":0,
            "taskId":"vagrant-test-http_example-8-a6cf7ec5-d793-49c7-b10f-0e14ab80bfff",
            "task": {
                "cachedHashCode":-1819348376,
                "job": {
                    "cachedHashCode":803049425,
                    "role":"vagrant",
                    "environment":"test",
                    "name":"http_example"
                    },
                "owner": {
                    "cachedHashCode":226895216,
                    "user":"vagrant"
                    },
                "isService":true,
                "numCpus":0.1,
                "ramMb":16,
                "diskMb":8,
                "priority":0,
                "maxTaskFailures":1,
                "production":false,
                "resources":[
                    {"cachedHashCode":729800451,"setField":"NUM_CPUS","value":0.1},
                    {"cachedHashCode":552899914,"setField":"RAM_MB","value":16},
                    {"cachedHashCode":-1547868317,"setField":"DISK_MB","value":8},
                    {"cachedHashCode":1957328227,"setField":"NAMED_PORT","value":"http"},
                    {"cachedHashCode":1954229436,"setField":"NAMED_PORT","value":"tcp"}
                    ],
                "constraints":[],
                "requestedPorts":["http","tcp"],
                "taskLinks":{"http":"http://%host%:%port:http%"},
                "contactEmail":"vagrant@localhost",
                "executorConfig": {
                    "cachedHashCode":-1194797325,
                    "name":"AuroraExecutor",
                    "data": "{\"environment\": \"test\", \"health_check_config\": {\"initial_interval_secs\": 5.0, \"health_checker\": { \"http\": {\"expected_response_code\": 0, \"endpoint\": \"/health\", \"expected_response\": \"ok\"}}, \"max_consecutive_failures\": 0, \"timeout_secs\": 1.0, \"interval_secs\": 1.0}, \"name\": \"http_example\", \"service\": true, \"max_task_failures\": 1, \"cron_collision_policy\": \"KILL_EXISTING\", \"enable_hooks\": false, \"cluster\": \"devcluster\", \"task\": {\"processes\": [{\"daemon\": false, \"name\": \"echo_ports\", \"ephemeral\": false, \"max_failures\": 1, \"min_duration\": 5, \"cmdline\": \"echo \\\"tcp port: {{thermos.ports[tcp]}}; http port: {{thermos.ports[http]}}; alias: {{thermos.ports[alias]}}\\\"\", \"final\": false}, {\"daemon\": false, \"name\": \"stage_server\", \"ephemeral\": false, \"max_failures\": 1, \"min_duration\": 5, \"cmdline\": \"cp /vagrant/src/test/sh/org/apache/aurora/e2e/http_example.py .\", \"final\": false}, {\"daemon\": false, \"name\": \"run_server\", \"ephemeral\": false, \"max_failures\": 1, \"min_duration\": 5, \"cmdline\": \"python http_example.py {{thermos.ports[http]}}\", \"final\": false}], \"name\": \"http_example\", \"finalization_wait\": 30, \"max_failures\": 1, \"max_concurrency\": 0, \"resources\": {\"disk\": 8388608, \"ram\": 16777216, \"cpu\": 0.1}, \"constraints\": [{\"order\": [\"echo_ports\", \"stage_server\", \"run_server\"]}]}, \"production\": false, \"role\": \"vagrant\", \"contact\": \"vagrant@localhost\", \"announce\": {\"primary_port\": \"http\", \"portmap\": {\"alias\": \"http\"}}, \"lifecycle\": {\"http\": {\"graceful_shutdown_endpoint\": \"/quitquitquit\", \"port\": \"health\", \"shutdown_endpoint\": \"/abortabortabort\"}}, \"priority\": 0}"},
                    "metadata":[],
                    "container":{
                        "cachedHashCode":-1955376216,
                        "setField":"MESOS",
                        "value":{"cachedHashCode":31}}
                    },
                    "assignedPorts":{},
                    "instanceId":8
        },
        "status":"PENDING",
        "failureCount":0,
        "taskEvents":[
            {"cachedHashCode":0,"timestamp":1464992060258,"status":"PENDING","scheduler":"aurora"}]
        },
        "oldState":{}}
```
