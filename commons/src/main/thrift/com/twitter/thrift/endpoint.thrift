// Author: jsirois

// TODO(wickman)  Fix uses of this in python+science!  Especially expertsearch
//  This should be aliased against twitter.thrift or twitter.common.service, or
//  wherever the python service discovery stack lands.

namespace java com.twitter.thrift
#@namespace scala com.twitter.thrift.endpoint.thriftscala
namespace rb Twitter.Thrift
namespace py gen.twitter.thrift.endpoint

/*
 * Represents the status of a service.
 */
enum Status {

  /*
   * The service is dead and can no longer be contacted.
   */
  DEAD = 0,

  /*
   * The service is in the process of starting up for the first time or from a STOPPED state.
   */
  STARTING = 1,

  /*
   * The service is alive and ready to receive requests.
   */
  ALIVE = 2,

  /*
   * The service is in the process of stopping and should no longer be contacted.  In this state
   * well behaved services will typically finish existing requests but accept no new rtequests.
   */
  STOPPING = 3,

  /*
   * The service is stopped and cannot be contacted unless started again.
   */
  STOPPED = 4,

  /*
   * The service is alive but in a potentially bad state.
   */
  WARNING = 5,
}

/*
 * Represents a TCP service network endpoint.
 */
struct Endpoint {

  /*
   * The remote hostname or ip address of the endpoint.
   */
  1: string host

  /*
   * The TCP port the endpoint listens on.
   */
  2: i32 port
}

/*
 * Represents information about the state of a service instance.
 */
struct ServiceInstance {

  /*
   * Represents the primary service interface endpoint.  This is typically a thrift service
   * endpoint.
   */
  1: Endpoint serviceEndpoint

  /*
   * A mapping of any additional interfaces the service exports.  The mapping is from logical
   * interface names to endpoints.  The map may be empty, but a typical additional endpoint mapping
   * would provide the endoint got the "http-admin" debug interface for example.
   *
   * TODO(John Sirois): consider promoting string -> Enum or adding thrift string constants for common
   * service names to help identify common beasts like ostrich-http-admin, ostrich-telnet and
   * process-http-admin but still allow for new experimental interfaces as well without having to
   * change this thift file.
   */
  2: map<string, Endpoint> additionalEndpoints

  /*
   * The status of this service instance.
   * NOTE: Only status ALIVE should be used. This field is pending removal.
   * TODO(Sathya Hariesh): Remove the status field.
   */
  3: Status status;

  /*
   * The shard identifier for this instance.
   */
  4: optional i32 shard;
}
