#!/usr/bin/env python
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
from __future__ import print_function

import os
import re
import sys
from optparse import OptionParser


class Type(object):
  '''A data type.'''

  def __init__(self, name, package=None, immutable=False):
    self.name = name
    self.package = package
    self.immutable = immutable

  def absolute_name(self):
    return '%s.%s' % (self.package, self.name) if self.package else self.name

  def __str__(self):
    return '%s (%smutable)' % (self.absolute_name(), 'im' if self.immutable else '')


class PrimitiveType(Type):
  '''A primitive type, with its associated typeboxed name.'''

  def __init__(self, name, boxed_name):
    Type.__init__(self, name, package=None, immutable=True)
    self.boxed_name = boxed_name


class ParameterizedType(Type):
  '''A parameterized type, usually a collection.'''

  def __init__(self, name, params):
    Type.__init__(self, name, None)
    self.params = params

  def param_names(self):
    def name(t):
      if isinstance(t, StructType):
        return t.codegen_name
      elif isinstance(t, PrimitiveType):
        return t.boxed_name
      else:
        return t.name
    return ', '.join([name(p) for p in self.params])


class StructType(Type):
  '''A thrift-defined type, which composes other types as fields.'''

  def __init__(self, name, package, kind, fields):
    Type.__init__(self, name, package, kind == 'enum')
    self.codegen_name = 'I%s' % name
    self.kind = kind
    self.fields = fields

  def __str__(self):
    return '%s %s { %s }' % (self.kind, self.name, ', '.join(map(str, self.fields)))


class Field(object):
  '''A field within a thrift structure.'''

  def __init__(self, ttype, name):
    self.ttype = ttype
    self.name = name

  def accessor_method(self):
    return '%s%s' % (
        'is' if self.ttype.name == 'boolean' else 'get',
        self.name[:1].capitalize() + self.name[1:])

  def isset_method(self):
    return 'isSet%s' % (self.name[0].upper() + self.name[1:])

  def __str__(self):
    return '%s: %s' % (self.name, self.ttype)


FIELD_TEMPLATE = '''  public %(type)s %(fn_name)s() {
    return %(field)s;
  }'''


# Template string for a method to access an immutable field.
IMMUTABLE_FIELD_TEMPLATE = '''  public %(type)s %(fn_name)s() {
    return wrapped.%(fn_name)s();
  }'''


STRUCT_DECLARATION = '''private final %(type)s %(field)s;'''
STRUCT_ASSIGNMENT = '''this.%(field)s = !wrapped.%(isset)s()
        ? null
        : %(type)s.buildNoCopy(wrapped.%(fn_name)s());'''


IMMUTABLE_COLLECTION_DECLARATION = (
    '''private final Immutable%(collection)s<%(params)s> %(field)s;''')
IMMUTABLE_COLLECTION_ASSIGNMENT = '''this.%(field)s = !wrapped.%(isset)s()
        ? Immutable%(collection)s.<%(params)s>of()
        : Immutable%(collection)s.copyOf(wrapped.%(fn_name)s());'''


# Template string for assignment for a collection field containing a struct.
STRUCT_COLLECTION_FIELD_ASSIGNMENT = '''this.%(field)s = !wrapped.%(isset)s()
        ? Immutable%(collection)s.<%(params)s>of()
        : FluentIterable.from(wrapped.%(fn_name)s())
              .transform(%(params)s.FROM_BUILDER)
              .to%(collection)s();'''

PACKAGE_NAME = 'org.apache.aurora.scheduler.storage.entities'

CLASS_TEMPLATE = '''/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package %(package)s;

%(imports)s

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 */
public final class %(name)s {
  private final %(wrapped)s wrapped;
  private int cachedHashCode = 0;
%(fields)s
  private %(name)s(%(wrapped)s wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);%(assignments)s
  }

  static %(name)s buildNoCopy(%(wrapped)s wrapped) {
    return new %(name)s(wrapped);
  }

  public static %(name)s build(%(wrapped)s wrapped) {
    return buildNoCopy(wrapped.deepCopy());
  }

  public static final Function<%(name)s, %(wrapped)s> TO_BUILDER =
      new Function<%(name)s, %(wrapped)s>() {
        @Override
        public %(wrapped)s apply(%(name)s input) {
          return input.newBuilder();
        }
      };

  public static final Function<%(wrapped)s, %(name)s> FROM_BUILDER =
      new Function<%(wrapped)s, %(name)s>() {
        @Override
        public %(name)s apply(%(wrapped)s input) {
          return new %(name)s(input);
        }
      };

  public static ImmutableList<%(wrapped)s> toBuildersList(Iterable<%(name)s> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<%(name)s> listFromBuilders(Iterable<%(wrapped)s> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<%(wrapped)s> toBuildersSet(Iterable<%(name)s> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<%(name)s> setFromBuilders(Iterable<%(wrapped)s> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public %(wrapped)s newBuilder() {
    return wrapped.deepCopy();
  }

%(accessors)s

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof %(name)s)) {
      return false;
    }
    %(name)s other = (%(name)s) o;
    return wrapped.equals(other.wrapped);
  }

  @Override
  public int hashCode() {
    // Following java.lang.String's example of caching hashCode.
    // This is thread safe in that multiple threads may wind up
    // computing the value, which is apparently favorable to constant
    // synchronization overhead.
    if (cachedHashCode == 0) {
      cachedHashCode = wrapped.hashCode();
    }
    return cachedHashCode;
  }

  @Override
  public String toString() {
    return wrapped.toString();
  }
}'''


class GeneratedCode(object):
  def __init__(self, class_name, wrapped_type):
    self._class_name = class_name
    self._wrapped_type = wrapped_type
    self._imports = set()
    self._accessors = []
    self._fields = []
    self._assignments = []

  def add_import(self, import_class):
    self._imports.add(import_class)

  def add_assignment(self, field, assignment):
    self._fields.append(field)
    self._assignments.append(assignment)

  def add_accessor(self, accessor_method):
    self._accessors.append(accessor_method)

  def dump(self, f):
    remaining_imports = list(self._imports)
    import_groups = []
    def remove_by_prefix(prefix):
      group = [i for i in remaining_imports if i.startswith(prefix)]
      remaining_imports[:] = [i for i in remaining_imports if not i.startswith(prefix)]
      return group

    def add_import_group(group):
      if group:
        import_groups.append('\n'.join(['import %s;' % i for i in sorted(group)]))

    twitter_imports = remove_by_prefix('com.twitter')
    add_import_group(remove_by_prefix('java'))
    add_import_group(remove_by_prefix('com'))
    add_import_group(remove_by_prefix('net'))
    add_import_group(remove_by_prefix('org'))
    add_import_group(twitter_imports)

    print(CLASS_TEMPLATE % {
      'package': PACKAGE_NAME,
      'name': self._class_name,
      'wrapped': self._wrapped_type,
      'imports': '\n\n'.join(import_groups),
      'accessors': '\n\n'.join(self._accessors),
      'fields': ('  ' + '\n  '.join(self._fields) + '\n') if self._fields else '',
      'assignments': ('\n    ' + '\n    '.join(self._assignments)) if self._assignments else '',
    }, file=f)


# A namespace declaration, e.g.:
#    namespace java org.apache.aurora.gen
NAMESPACE_RE = 'namespace\s+(?P<lang>\w+)\s+(?P<namespace>[^\s]+)'

# A possibly-parameterized type name, e.g.:
#    int
#    TaskConfig
#    Set<String>
#    Map<String, TaskConfig>
TYPE_PATTERN = '(?P<type>\w+)(?:<(?P<params>[^>]+)>)?'


# Matches a complete struct definnition, capturing the type and body.
STRUCT_RE = '(?P<kind>enum|struct|union)\s+(?P<name>\w+)\s+{(?P<body>[^}]+)}'


# A field definition within a struct, e.g.:
#     1: string name
#     15: Map<String, TaskConfig> configs  # Configs mapped by name.
FIELD_RE = '\s*\d+:\s+(?:(?:required|optional)\s+)?(%s)\s+(?P<name>\w+).*' % TYPE_PATTERN

THRIFT_TYPES = {
  'bool': PrimitiveType('boolean', 'Boolean'),
  'i32': PrimitiveType('int', 'Integer'),
  'i64': PrimitiveType('long', 'Long'),
  'double': PrimitiveType('double', 'Double'),
  'string': PrimitiveType('String', 'String'),
  'list': Type('List'),
  'set': Type('Set'),
  'map': Type('Map'),
  'binary': PrimitiveType('byte[]', 'byte[]'),
}

def parse_structs(thrift_defs):
  '''Read all thrift structures found in a file.

  This returns a list of Type objects representing the structs found
  and the fields they contain.
  '''
  # Capture all namespace definitions.
  namespaces = dict(re.findall(NAMESPACE_RE, thrift_defs))

  # Keep track of structs already seen, to identify referenced types.
  structs = []

  def parse_field(field):
    def make_type(name):
      if name in ['list', 'map', 'set']:
        return Type(name.title())
      elif name in THRIFT_TYPES:
        return THRIFT_TYPES[name]
      else:
        return [s for s in structs if s.name == name][0]

    type_name = field.group('type')
    type_params = field.group('params')
    if type_params:
      params = [make_type(p) for p in type_params.replace(' ', '').split(',')]
      ttype = ParameterizedType(type_name.title(), params)
    else:
      ttype = make_type(type_name)
    return Field(ttype, field.group('name'))

  def parse_fields(field_str):
    return map(parse_field, re.finditer(FIELD_RE, field_str))

  for s in re.finditer(STRUCT_RE, thrift_defs, flags=re.MULTILINE):
    structs.append(StructType(s.group('name'),
                              namespaces['java'],
                              s.group('kind'),
                              parse_fields(s.group('body'))))
  return structs


def generate_java(struct):
  code = GeneratedCode(struct.codegen_name, struct.name)
  code.add_import('com.google.common.base.Preconditions')
  code.add_import('com.google.common.base.Function')
  code.add_import('com.google.common.collect.ImmutableList')
  code.add_import('com.google.common.collect.ImmutableSet')
  code.add_import('com.google.common.collect.FluentIterable')
  code.add_import(struct.absolute_name())

  if struct.kind == 'union':
    code.add_accessor(IMMUTABLE_FIELD_TEMPLATE
                      % {'type': '%s._Fields' % struct.name, 'fn_name': 'getSetField'})

  # Accessor for each field.
  for field in struct.fields:
    code.add_accessor(IMMUTABLE_FIELD_TEMPLATE
                      % {'type': 'boolean',
                         'fn_name': field.isset_method()})
    if field.ttype.immutable:
      code.add_accessor(IMMUTABLE_FIELD_TEMPLATE % {'type': field.ttype.name,
                                                    'fn_name': field.accessor_method()})
    elif not struct.kind == 'union':
      if isinstance(field.ttype, StructType):
        return_type = field.ttype.codegen_name
      elif isinstance(field.ttype, ParameterizedType):
        # Add imports for any referenced enum types. This is not necessary for other
        # types since they are either primitives or struct types, which will be in
        # the same package.
        for param_type in field.ttype.params:
          if isinstance(param_type, StructType) and param_type.kind == 'enum':
            code.add_import(param_type.absolute_name())

        return_type = 'Immutable%s<%s>' % (field.ttype.name, field.ttype.param_names())
      else:
        return_type = field.ttype.name
      code.add_accessor(FIELD_TEMPLATE % {'type': return_type,
                                          'fn_name': field.accessor_method(),
                                          'field': field.name})

    if isinstance(field.ttype, StructType):
      if field.ttype.kind == 'enum':
        code.add_import(field.ttype.absolute_name())

      if field.ttype.immutable:
        # Direct accessor was already added.
        pass
      elif struct.kind == 'union':
        copy_field = '%s.build(wrapped.%s())' % (field.ttype.codegen_name,
                                                 field.accessor_method())
        code.add_accessor(FIELD_TEMPLATE % {'type': field.ttype.codegen_name,
                                            'fn_name': field.accessor_method(),
                                            'field': copy_field})
      else:
        args = {
          'field': field.name,
          'fn_name': field.accessor_method(),
          'isset': field.isset_method(),
          'type': field.ttype.codegen_name,
        }
        code.add_assignment(STRUCT_DECLARATION % args, STRUCT_ASSIGNMENT % args)
    elif isinstance(field.ttype, ParameterizedType):
      # Add necessary imports, supporting only List, Map, Set.
      assert field.ttype.name in ['List', 'Map', 'Set'], 'Unrecognized type %s' % field.ttype.name
      code.add_import('com.google.common.collect.Immutable%s' % field.ttype.name)

      params = field.ttype.params
      if all([p.immutable for p in params]):
        # All parameter types are immutable.
        assignment = IMMUTABLE_COLLECTION_ASSIGNMENT
      elif len(params) == 1:
        # Only one non-immutable parameter.
        # Assumes the parameter type is a struct and our code generator
        # will make a compatible wrapper class and constructor.
        assignment = STRUCT_COLLECTION_FIELD_ASSIGNMENT
      else:
        assert False, 'Unable to codegen accessor field for %s' % field.name
      args = {'collection': field.ttype.name,
              'field': field.name,
              'fn_name': field.accessor_method(),
              'isset': field.isset_method(),
              'params': field.ttype.param_names()}
      code.add_assignment(IMMUTABLE_COLLECTION_DECLARATION % args, assignment % args)
  return code

if __name__ == '__main__':
  parser = OptionParser()
  parser.add_option('-v', '--verbose',
                    dest='verbose',
                    action='store_true',
                    help='Display extra information about code generation.')
  options, args = parser.parse_args()

  def log(value):
    if options.verbose:
      print(value)

  if len(args) != 2:
    print('usage: %s thrift_file output_directory' % sys.argv[0])
    sys.exit(1)

  thrift_file, output_directory = args
  with open(thrift_file) as f:
    # Load all structs found in the thrift file.
    structs = parse_structs(f.read())

    package_dir = os.path.join(output_directory, PACKAGE_NAME.replace('.', os.path.sep))
    if not os.path.isdir(package_dir):
      os.makedirs(package_dir)
    for struct in structs:
      # Skip generation for enums, since they are immutable.
      if struct.kind == 'enum':
        continue
      gen_file = os.path.join(package_dir, '%s.java' % struct.codegen_name)
      log('Generating %s' % gen_file)
      with open(gen_file, 'w') as f:
        code = generate_java(struct)
        code.dump(f)
