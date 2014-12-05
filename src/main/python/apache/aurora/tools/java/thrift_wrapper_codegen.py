#!/usr/bin/env python2.7
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
# checkstyle: noqa
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
        return t.name if t.kind == 'enum' else t.codegen_name
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

class EnumType(StructType):
  '''A thrift-defined value enumeration.'''

  def __init__(self, name, package, values):
    StructType.__init__(self, name, package, 'enum', [])
    self.values = values

  def __str__(self):
    return '%s (%s)' % (self.name, ', '.join(self.values))

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


CLASS_TEMPLATE = '''package %(package)s;

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
    this.wrapped = Objects.requireNonNull(wrapped);%(assignments)s
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
          return %(name)s.build(input);
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

# Matches a complete struct definition, capturing the type and body.
STRUCT_RE = '(?P<kind>enum|struct|union)\s+(?P<name>\w+)\s+{(?P<body>[^}]+)}'

# A possibly-parameterized type name, e.g.:
#    int
#    TaskConfig
#    Set<String>
#    Map<String, TaskConfig>
TYPE_PATTERN = '(?P<type>\w+)(?:<(?P<params>[^>]+)>)?'

# A field definition within a struct, e.g.:
#     1: string name
#     15: Map<String, TaskConfig> configs  # Configs mapped by name.
FIELD_RE = '\s*\d+:\s+(?:(?:required|optional)\s+)?(%s)\s+(?P<name>\w+).*' % TYPE_PATTERN

# An enum value definition, e.g.:
#    INVALID_REQUEST = 0,
ENUM_VALUE_RE = '\s*(?P<name>\w+)\s*=\s*\d+,?'


class Service(object):
  def __init__(self, name, parent, methods):
    self.name = name
    self.parent = parent
    self.methods = methods

  def __str__(self):
    return ''.join([self.name, self.parent or '', '  ' + '\n  '.join(map(str, self.methods))])

class Method(object):
  def __init__(self, name, parameters, return_type):
    self.name = name
    self.parameters = parameters
    self.return_type = return_type

  def __str__(self):
    return '%s(%s)' % (self.name, ', '.join(map(str, self.parameters)))

class Parameter(object):
  def __init__(self, name, type_name):
    self.name = name
    self.type_name = type_name

  def __str__(self):
    return '%s %s' % (self.type_name, self.name)

class GenericParameter(Parameter):
  def __init__(self, name, type_name, parameters):
    Parameter.__init__(self, name, type_name)
    self.parameters = parameters

GET_SUPER_METHODS = '.putAll(%(super)sMetadata.METHODS)'

PARAM_METADATA_TEMPLATE = '.put("%(name)s", %(type)s.class)'

GENERIC_PARAM_METADATA_TEMPLATE = (
    '.put("%(name)s", new TypeToken<%(type)s<%(params)s>>() {}.getType())')

METHOD_METADATA_TEMPLATE = '''.put(
              "%(name)s",
              ImmutableMap.<String, Type>builder()%(params)s
                  .build())'''

SERVICE_METADATA_TEMPLATE = '''package %(package)s;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;

import org.apache.aurora.gen.*;

public final class %(name)sMetadata {
  public static final Map<String, Map<String, Type>> METHODS =
      ImmutableMap.<String, Map<String, Type>>builder()
          %(methods)s
          .build();

  private %(name)sMetadata() {
    // Utility class
  }
}
'''

SERVICE_RE = 'service (?P<name>\w+)\s+(extends\s+(?P<super>\w+)\s+)?{(?P<body>[^}]+)}'

METHOD_RE = '\s*(?P<return>\w+)\s+(?P<name>\w+)\((?P<params>[^\)]*)\)'

PARAM_RE = '\d+\:\s+%s\s+(?P<name>\w+)' % TYPE_PATTERN

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

  def parse_values(enum_str):
    return [m.group('name') for m in re.finditer(ENUM_VALUE_RE, enum_str)]

  for s in re.finditer(STRUCT_RE, thrift_defs, flags=re.MULTILINE):
    if s.group('kind') == 'enum':
      struct = EnumType(s.group('name'),
                        namespaces.get('java', ''),
                        parse_values(s.group('body')))
    else:
      struct = StructType(s.group('name'),
                          namespaces.get('java', ''),
                          s.group('kind'),
                          parse_fields(s.group('body')))
    structs.append(struct)

  return structs


def parse_services(service_defs):
  services = []

  for s in re.finditer(SERVICE_RE, service_defs, flags=re.MULTILINE):
    methods = []
    for method in re.finditer(METHOD_RE, s.group('body'), flags=re.MULTILINE):
      params = []
      for param in re.finditer(PARAM_RE, method.group('params'), flags=re.MULTILINE):
        if param.group('params'):
          params.append(GenericParameter(
              param.group('name'),
              param.group('type'),
              param.group('params').replace(' ', '').split(',')))
        else:
          params.append(Parameter(param.group('name'), param.group('type')))
      methods.append(Method(method.group('name'),
                            params,
                            method.group('return')))
    services.append(Service(s.group('name'), s.group('super'), methods))
  return services


def generate_java(struct):
  code = GeneratedCode(struct.codegen_name, struct.name)
  code.add_import('java.util.Objects')
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

  if len(args) != 3:
    print('usage: %s thrift_file code_output_dir resource_output_dir' % sys.argv[0])
    sys.exit(1)

  thrift_file, code_output_dir, resource_output_dir = args
  with open(thrift_file) as f:
    # Load all structs found in the thrift file.
    file_contents = f.read()
    services = parse_services(file_contents)
    if not services:
      log('Skipping generation for %s since there are no services.' % thrift_file)
      sys.exit(0)
    structs = parse_structs(file_contents)

    package_dir = os.path.join(code_output_dir, PACKAGE_NAME.replace('.', os.path.sep))
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

    resource_dir = os.path.join(resource_output_dir, PACKAGE_NAME.replace('.', os.path.sep), 'help')
    if not os.path.isdir(resource_dir):
      os.makedirs(resource_dir)

    methods_dir = os.path.join(resource_dir, 'method')
    if not os.path.isdir(methods_dir):
      os.makedirs(methods_dir)
    types_dir = os.path.join(resource_dir, 'type')
    if not os.path.isdir(types_dir):
      os.makedirs(types_dir)

    def get_service(name):
      return [s for s in services if s.name == name][0]

    service = get_service('AuroraAdmin')

    all_methods = [] + service.methods
    cur_service = service
    while cur_service.parent:
      cur_service = get_service(cur_service.parent)
      all_methods += cur_service.methods

    def get_type_name(name):
      if name in THRIFT_TYPES:
        thrift_type = THRIFT_TYPES[name]
        if isinstance(thrift_type, PrimitiveType):
          return thrift_type.boxed_name
        else:
          return name
      return name

    def add_param(param):
      if param.type_name in THRIFT_TYPES:
        thrift_type = THRIFT_TYPES[param.type_name]
        if not isinstance(thrift_type, PrimitiveType):
          return GENERIC_PARAM_METADATA_TEMPLATE % {
            'name': param.name,
            'type': thrift_type.name,
            'params': ', '.join(map(get_type_name, param.parameters))
          }
      return PARAM_METADATA_TEMPLATE % {
        'name': param.name,
        'type': get_type_name(param.type_name)
      }

    def add_method(method):
      spacing = '\n                  '
      return METHOD_METADATA_TEMPLATE % {
        'name': method.name,
        'params': (spacing if method.parameters else '') + spacing.join(map(add_param, method.parameters))
      }

    method_metadata = '\n          '.join(map(add_method, all_methods))

    service_metadata = SERVICE_METADATA_TEMPLATE % {
            'package': PACKAGE_NAME,
            'methods': method_metadata,
            'name': service.name
          }
    gen_file = os.path.join(package_dir, '%sMetadata.java' % service.name)
    log('Generating service metadata file %s' % gen_file)
    with open(gen_file, 'w') as f:
      print(service_metadata, file=f)
