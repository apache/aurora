#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import sys

# A new log level, for information that's slightly more important than debugging.
# This is the level of log information that will be sent to a distibuted
# log to gather information about user behavior.
TRANSCRIPT = logging.DEBUG + 1
logging.addLevelName(TRANSCRIPT, "TRANSCRIPT")

class PlainFormatter(logging.Formatter):
  """
    Format a log in a simple style:
    type] msg
  """
  SCHEME = "plain"

  LEVEL_MAP = {
    logging.FATAL: "FATAL",
    logging.ERROR: "ERROR",
    logging.WARN:  "WARN",
    logging.INFO:  "info",
    TRANSCRIPT: "transcript",
    logging.DEBUG: "debug"
  }

  def __init__(self):
    logging.Formatter.__init__(self)

  def format(self, record):
    try:
      record_message = "%s" % (record.msg % record.args)
    except TypeError:
      record_message = record.msg
    try:
      level = PlainFormatter.LEVEL_MAP[record.levelno]
    except:
      level = "?????"
    record_message = "log(%s): %s" % (level, record_message)
    record.getMessage = lambda: record_message
    return logging.Formatter.format(self, record)


def setup_default_log_handlers(level):
  stderr_handler = logging.StreamHandler(sys.stderr)
  stderr_handler.setLevel(level)
  stderr_handler.setFormatter(PlainFormatter())
  root_logger = logging.getLogger()
  root_logger.addHandler(stderr_handler)
  root_logger.setLevel(logging.INFO)
