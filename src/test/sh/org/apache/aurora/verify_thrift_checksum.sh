#!/bin/bash
#
# Copyright 2013 Apache Software Foundation
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
set -o nounset

# A test to remind us that we need to exercise care when making API and
# storage schema changes.
# There are two operating modes: verify and reset.  Running the script with
# no arguments will use verify mode.  Reset mode will overwrite all golden
# files with the current checksum of the source files.
# In other words: immediately after running in reset mode, the test should
# pass in verify mode.

reset_mode=false
while getopts "r" opt
do
  case ${opt} in
    r) reset_mode=true ;;
    *) exit 1;
  esac
done

if $reset_mode
then
  echo 'Warning: Operating in reset mode. Rewriting golden checksum files.'
fi

for file in $(find src/main/thrift -type f -name '*.thrift')
do
  # Using python for lack of a better cross-platform checksumming without
  # adding another build-time dependency.
  checksum=$(python << EOF
import hashlib
with open('$file', 'rb') as f:
  print(hashlib.md5(f.read()).hexdigest())
EOF)
  golden_file=src/test/resources/${file#src/main/thrift/}.md5
  if $reset_mode
  then
    mkdir -p $(dirname "$golden_file")
    echo "$checksum" > "$golden_file"
  else
    if [ ! -f "$golden_file" ]
    then
      echo "Golden file not found for $file, expected $golden_file"
      exit 1
    fi
    golden_checksum=$(cat "$golden_file")
    if [ "$checksum" != "$golden_checksum" ]
    then
      echo "Golden checksum did not match for $file"
      echo "Found $checksum, expected $golden_checksum"
      echo
      echo $(printf '!%.0s' {1..80})
      echo 'This means you changed a thrift file.'
      echo 'Please think carefully before you proceed!'
      echo
      echo 'If you are changing an API or a storage schema you may need to '
      echo 'take additional actions to such as providing a client and/or '
      echo 'server-side migration strategy.  You may also need to bump the '
      echo 'released version ID, following the guidelines at http://semver.org'
      echo
      echo 'This test is not here to help you make those changes, but to '
      echo 'remind you to take appropriate follow-up actions relating to your '
      echo 'schema change.'
      echo
      echo 'Once you are confident that you have appropriately supported this '
      echo 'change in relevant code, you can fix this test by running '
      echo "sh $0 -r"
      echo $(printf '!%.0s' {1..80})
      exit 1
    fi
  fi
done

if ! $reset_mode
then
  echo "All checksums match."
fi
exit 0
