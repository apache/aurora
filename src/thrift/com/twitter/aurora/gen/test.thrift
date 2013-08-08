// Copyright 2013 Twitter, Inc.

namespace java com.twitter.aurora.gen.test
namespace py gen.twitter.mesos.test

// Test data for Thrift interface definition for the Twitter Mesos Scheduler.

// Test data for job path identifiers.
const set<string> VALID_IDENTIFIERS = ["devel",
                                       "dev-prod",
                                       "Dev_prod-",
                                       "deV.prod",
                                       ".devprod.."]

const set<string> INVALID_IDENTIFIERS = ["dev/prod",
                                         "dev prod",
                                         "/dev/prod",
                                         "///",
                                         "new\nline",
                                         "    hello world."]
