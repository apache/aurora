// Thrift structures for a local log storage system, for use in simulated environments.
namespace java com.twitter.aurora.gen.test

struct LogRecord {
  1: binary contents
}

struct FileLogContents {
  1: map<i64, LogRecord> records
}
