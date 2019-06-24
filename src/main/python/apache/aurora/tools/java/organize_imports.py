#!/bin/env python
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

'''
Organizes a java source file's import statements in a way that pleases
Apache Aurora's checkstyle configuration.  This expects exactly one
argument: the name of the file to modify with preferred import ordering.
'''

from __future__ import print_function

import re
import sys
from collections import defaultdict

IMPORT_RE = re.compile('import(?: static)? (.*);')


def get_group(import_statement):
  matcher = IMPORT_RE.match(import_statement)
  assert matcher, 'Could not parse import statement: %s' % import_statement
  class_name = matcher.group(1)
  group = class_name.split('.')[0]
  return group


def index_by_group(import_statements):
  groups = defaultdict(list)
  for line in import_statements:
    groups[get_group(line)].append(line)
  return groups


IMPORT_CLASS_RE = re.compile(
    r'import(?: static)? (?P<outer>[^A-Z]*[A-Z]\w+)(?:\.(?P<inners>[\w][^;]*))?')


def get_all_group_lines(import_groups):
  if not import_groups:
    return []

  def get_group_lines(group):
    def comparator(x, y):
      # These shenanigans are used to properly order imports for inner classes.
      # So we get ordering like:
      # import com.foo.Bar;
      # import com.foo.Bar.Baz;
      # (this is not lexicographical, so normal sort won't suffice)
      x_m = IMPORT_CLASS_RE.match(x)
      y_m = IMPORT_CLASS_RE.match(y)
      if x_m.group('outer') == y_m.group('outer'):
        return cmp(x_m.group('inners'), y_m.group('inners'))
      else:
        return cmp(x, y)
    lines = sorted(import_groups[group], comparator)
    lines.append('')
    return lines

  all_lines = []
  explicit_groups = ['java', 'javax', 'scala', 'com', 'net', 'org']
  for group in explicit_groups:
    if group in import_groups:
      all_lines += get_group_lines(group)

  # Gather remaining groups.
  remaining_groups = sorted(set(import_groups.keys()) - set(explicit_groups))
  for group in remaining_groups:
    all_lines += get_group_lines(group)
  return all_lines


BEFORE_IMPORTS = 'before_imports'
IMPORTS = 'imports'
STATIC_IMPORTS = 'static_imports'
AFTER_IMPORTS = 'after_imports'


def main(argv):
  if len(argv) != 2:
    print('usage: %s FILE' % argv[0])
    sys.exit(1)

  print('Organizing imports in %s' % argv[1])
  lines_before_imports = []
  import_lines = []
  static_import_lines = []
  lines_after_imports = []
  with open(argv[1], 'r') as f:
    position = BEFORE_IMPORTS
    for line in f:
      line = line.rstrip()
      if position == BEFORE_IMPORTS:
        if line.startswith('import'):
          position = IMPORTS
        else:
          lines_before_imports.append(line)
      if position == IMPORTS:
        if line.startswith('import static'):
          position = STATIC_IMPORTS
        elif line.startswith('import'):
          import_lines.append(line)
        elif line.strip():
          position = AFTER_IMPORTS
      if position == STATIC_IMPORTS:
        if line.startswith('import static'):
          static_import_lines.append(line)
        elif line.strip():
          position = AFTER_IMPORTS
      if position == AFTER_IMPORTS:
        lines_after_imports.append(line)

  import_groups = index_by_group(import_lines)
  static_import_groups = index_by_group(static_import_lines)

  def ensure_line_padding(lines):
    if lines and lines[-1] != '':
      lines.append('')
    return lines

  file_lines = lines_before_imports
  if import_groups:
    ensure_line_padding(file_lines)
    file_lines += get_all_group_lines(import_groups)
  if static_import_groups:
    ensure_line_padding(file_lines)
    file_lines += get_all_group_lines(static_import_groups)
  if lines_after_imports:
    ensure_line_padding(file_lines)
    file_lines += lines_after_imports

  with open(argv[1], 'w') as f:
    for line in file_lines:
      print(line, file=f)


if __name__ == '__main__':
  main(sys.argv)
