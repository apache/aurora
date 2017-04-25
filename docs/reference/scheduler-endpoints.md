# HTTP endpoints

There are a number of HTTP endpoints that the Aurora scheduler exposes. These allow various
operational tasks to be performed on the scheduler. Below is an (incomplete) list of such endpoints
and a brief explanation of what they do.

## Leader health
The /leaderhealth endpoint enables performing health checks on the scheduler instances inorder
to forward requests to the leading scheduler. This is typically used by a load balancer such as
HAProxy or AWS ELB.

When a HTTP GET request is issued on this endpoint, it responds as follows:

- If the instance that received the GET request is the leading scheduler, a HTTP status code of
  `200 OK` is returned.
- If the instance that received the GET request is not the leading scheduler but a leader does
  exist, a HTTP status code of `503 SERVICE_UNAVAILABLE` is returned.
- If no leader currently exists or the leader is unknown, a HTTP status code of `502 BAD_GATEWAY`
  is returned.
