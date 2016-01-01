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

  def codegen_name(self):
    return self.name

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
      if isinstance(t, StructType) and not t.immutable:
        return t.codegen_name()
      elif isinstance(t, PrimitiveType):
        return t.boxed_name
      else:
        return t.name
    return ', '.join([name(p) for p in self.params])


class StructType(Type):
  '''A thrift-defined type, which composes other types as fields.'''

  def __init__(self, name, package, kind, fields):
    Type.__init__(self, name, package, kind == 'enum')
    self.kind = kind
    self.fields = fields

  def codegen_name(self):
    return 'I%s' % self.name

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

  def capitalized_name(self):
    return self.name[:1].capitalize() + self.name[1:]

  def accessor_method(self):
    return '%s%s' % (
        'is' if self.ttype.name == 'boolean' else 'get',
        self.capitalized_name())

  def isset_method(self):
    return 'isSet%s' % (self.name[0].upper() + self.name[1:])

  def __str__(self):
    return '%s: %s' % (self.name, self.ttype)


FIELD_TEMPLATE = '''  public %(type)s %(fn_name)s() {
    return %(field)s;
  }'''

UNION_FIELD_TEMPLATE = '''  public %(type)s %(fn_name)s() {
    if (getSetField() == %(enum_value)s) {
      return (%(type)s) value;
    } else {
      throw new RuntimeException("Cannot get field '%(enum_value)s' "
          + "because union is currently set to " + getSetField());
    }
  }'''

UNION_SWITCH_CASE = '''case %(case)s:
        %(body)s'''

UNION_FIELD_SWITCH = '''switch (getSetField()) {
      %(cases)s
      default:
        throw new RuntimeException("Unrecognized field " + getSetField());
    }'''

SIMPLE_ASSIGNMENT = 'this.%(field)s = wrapped.%(fn_name)s();'

FIELD_DECLARATION = '''private final %(type)s %(field)s;'''
STRUCT_ASSIGNMENT = '''this.%(field)s = wrapped.%(isset)s()
        ? %(type)s.build(wrapped.%(fn_name)s())
        : null;'''


IMMUTABLE_COLLECTION_DECLARATION = (
    '''private final Immutable%(collection)s<%(params)s> %(field)s;''')
IMMUTABLE_COLLECTION_ASSIGNMENT = '''this.%(field)s = wrapped.%(isset)s()
        ? Immutable%(collection)s.copyOf(wrapped.%(fn_name)s())
        : Immutable%(collection)s.of();'''


# Template string for assignment for a collection field containing a struct.
STRUCT_COLLECTION_FIELD_ASSIGNMENT = '''this.%(field)s = wrapped.%(isset)s()
        ? FluentIterable.from(wrapped.%(fn_name)s())
              .transform(%(params)s::build)
              .to%(collection)s()
        : Immutable%(collection)s.<%(params)s>of();'''

PACKAGE_NAME = 'org.apache.aurora.scheduler.storage.entities'


CLASS_TEMPLATE = '''package %(package)s;

%(imports)s

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 */
public final class %(name)s {
  private int cachedHashCode = 0;
%(fields)s
  private %(name)s(%(wrapped)s wrapped) {%(assignments)s
  }

  public static %(name)s build(%(wrapped)s wrapped) {
    return new %(name)s(wrapped);
  }

  public static ImmutableList<%(wrapped)s> toBuildersList(Iterable<%(name)s> w) {
    return FluentIterable.from(w).transform(%(name)s::newBuilder).toList();
  }

  static List<%(wrapped)s> toMutableBuildersList(Iterable<%(name)s> w) {
    return Lists.newArrayList(Iterables.transform(w, %(name)s::newBuilder));
  }

  public static ImmutableList<%(name)s> listFromBuilders(Iterable<%(wrapped)s> b) {
    return FluentIterable.from(b).transform(%(name)s::build).toList();
  }

  public static ImmutableSet<%(wrapped)s> toBuildersSet(Iterable<%(name)s> w) {
    return FluentIterable.from(w).transform(%(name)s::newBuilder).toSet();
  }

  static Set<%(wrapped)s> toMutableBuildersSet(Iterable<%(name)s> w) {
    return Sets.newHashSet(Iterables.transform(w, %(name)s::newBuilder));
  }

  public static ImmutableSet<%(name)s> setFromBuilders(Iterable<%(wrapped)s> b) {
    return FluentIterable.from(b).transform(%(name)s::build).toSet();
  }

  public %(wrapped)s newBuilder() {
    %(copy_constructor)s
  }

%(accessors)s

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof %(name)s)) {
      return false;
    }
    %(name)s other = (%(name)s) o;
    return %(equals)s;
  }

  @Override
  public int hashCode() {
    // Following java.lang.String's example of caching hashCode.
    // This is thread safe in that multiple threads may wind up
    // computing the value, which is apparently favorable to constant
    // synchronization overhead.
    if (cachedHashCode == 0) {
      cachedHashCode = Objects.hash(%(hashcode)s);
    }
    return cachedHashCode;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)%(to_string)s
        .toString();
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
    self.to_string = 'unset'
    self.hash_code = 'unset'
    self.equals = 'unset'
    self.builder = 'unset'
    self.copy_constructor = 'unset'

  def add_import(self, import_class):
    self._imports.add(import_class)

  def add_field(self, field):
    self._fields.append(field)

  def add_assignment(self, assignment):
    self._assignments.append(assignment)

  def add_accessor(self, accessor_method):
    self._accessors.append(accessor_method)

  def dump(self, out_file):
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
      'to_string': self.to_string,
      'equals': self.equals,
      'hashcode': self.hash_code,
      'copy_constructor': self.copy_constructor,
    }, file=out_file)


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
    return '%s(%s)' % (self.name, ', '.join(self.parameters))

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

PARAM_METADATA_TEMPLATE = '%(type)s.class,'

METHOD_METADATA_TEMPLATE = '''.put(
              "%(name)s",
              new Class<?>[] {%(params)s
                  })'''

SERVICE_METADATA_TEMPLATE = '''package %(package)s;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.apache.aurora.gen.*;

public final class %(name)sMetadata {
  public static final ImmutableMap<String, Class<?>[]> METHODS =
      ImmutableMap.<String, Class<?>[]>builder()
          %(methods)s
          .build();

  private %(name)sMetadata() {
    // Utility class
  }
}
'''

SERVICE_RE = 'service (?P<name>\w+)\s+(extends\s+(?P<super>\w+)\s+)?{(?P<body>[^}]+)}'

METHOD_RE = '\s*(?P<return>\w+)\s+(?P<name>\w+)\((?P<params>[^\)]*)\)'

PARAM_RE = '\d+\:\s+%s\s+(?:\w+)' % TYPE_PATTERN

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
        params.append(param.group('type'))
      methods.append(Method(method.group('name'),
                            params,
                            method.group('return')))
    services.append(Service(s.group('name'), s.group('super'), methods))
  return services


def to_upper_snake_case(s):
  return re.sub('([A-Z])', '_\\1', s).upper()


def generate_union_field(code, struct, field):
  field_enum_value = '%s._Fields.%s' % (struct.name, to_upper_snake_case(field.name))
  code.add_accessor(FIELD_TEMPLATE % {'type': 'boolean',
                                      'fn_name': field.isset_method(),
                                      'field': 'setField == %s' % field_enum_value})
  code.add_accessor(UNION_FIELD_TEMPLATE % {'type': field.ttype.codegen_name(),
                                            'fn_name': field.accessor_method(),
                                            'enum_value': field_enum_value})


def generate_struct_field(code, struct, field, builder_calls):
  field_type = field.ttype.codegen_name()
  assignment = SIMPLE_ASSIGNMENT
  assignment_args = {
    'field': field.name,
    'fn_name': field.accessor_method()
  }
  builder_assignment = field.name

  if field.ttype.immutable:
    code.add_accessor(FIELD_TEMPLATE % {'type': field.ttype.name,
                                        'fn_name': field.accessor_method(),
                                        'field': field.name})
  else:
    if isinstance(field.ttype, ParameterizedType):
      # Add imports for any referenced enum types. This is not necessary for other
      # types since they are either primitives or struct types, which will be in
      # the same package.
      for param_type in field.ttype.params:
        if isinstance(param_type, StructType) and param_type.kind == 'enum':
          code.add_import(param_type.absolute_name())

      field_type = 'Immutable%s<%s>' % (field.ttype.name, field.ttype.param_names())
    code.add_accessor(FIELD_TEMPLATE % {'type': field_type,
                                        'fn_name': field.accessor_method(),
                                        'field': field.name})

  if isinstance(field.ttype, StructType):
    if field.ttype.kind == 'enum':
      field_type = field.ttype.name
      code.add_import(field.ttype.absolute_name())

    if not field.ttype.immutable:
      assignment = STRUCT_ASSIGNMENT
      assignment_args = {
        'field': field.name,
        'fn_name': field.accessor_method(),
        'isset': field.isset_method(),
        'type': field.ttype.codegen_name(),
      }
      builder_assignment = '%s.newBuilder()' % field.name
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
      builder_assignment = '%s.toMutableBuilders%s(%s)' % (params[0].codegen_name(), field.ttype.name, field.name)
    else:
      assert False, 'Unable to codegen accessor field for %s' % field.name
    assignment_args = {'collection': field.ttype.name,
                       'field': field.name,
                       'fn_name': field.accessor_method(),
                       'isset': field.isset_method(),
                       'params': field.ttype.param_names()}

  code.add_field(FIELD_DECLARATION % {'field': field.name, 'type': field_type })

  nullable = field.ttype.name == 'String' or not isinstance(field.ttype, PrimitiveType)
  if nullable:
    code.add_accessor(FIELD_TEMPLATE % {'type': 'boolean',
                                        'fn_name': field.isset_method(),
                                        'field': '%s != null' % field.name})
    builder_calls.append('.set%s(%s == null ? null : %s)' % (field.capitalized_name(), field.name, builder_assignment))
  else:
    builder_calls.append('.set%s(%s)' % (field.capitalized_name(), builder_assignment))
  code.add_assignment(assignment % assignment_args)

def generate_java(struct):
  code = GeneratedCode(struct.codegen_name(), struct.name)
  code.add_import('java.util.Objects')
  code.add_import('java.util.List')
  code.add_import('java.util.Set')
  code.add_import('com.google.common.base.MoreObjects')
  code.add_import('com.google.common.collect.ImmutableList')
  code.add_import('com.google.common.collect.ImmutableSet')
  code.add_import('com.google.common.collect.FluentIterable')
  code.add_import('com.google.common.collect.Iterables')
  code.add_import('com.google.common.collect.Lists')
  code.add_import('com.google.common.collect.Sets')
  code.add_import(struct.absolute_name())

  if struct.kind == 'union':
    assign_cases = []
    copy_cases = []
    for field in struct.fields:
      generate_union_field(code, struct, field)

      assign_case_body = 'value = %(codegen_name)s.build(wrapped.%(accessor_method)s());\nbreak;' % {
          'codegen_name': field.ttype.codegen_name(),
          'accessor_method': field.accessor_method()}
      assign_cases.append(UNION_SWITCH_CASE % {'case': to_upper_snake_case(field.name),
                                               'body': assign_case_body})

      copy_case_body = 'return new %s(setField, %s().newBuilder());' % (struct.name, field.accessor_method())
      copy_cases.append(UNION_SWITCH_CASE % {'case': to_upper_snake_case(field.name),
                                             'body': copy_case_body})

    set_field_type = '%s._Fields' % struct.name
    code.add_accessor(FIELD_TEMPLATE % {'type': set_field_type, 'fn_name': 'getSetField', 'field': 'setField'})
    code.add_field(FIELD_DECLARATION % {'field': 'setField', 'type': set_field_type})
    code.add_assignment(SIMPLE_ASSIGNMENT % {'field': 'setField',
                                             'fn_name': 'getSetField'})
    code.add_field(FIELD_DECLARATION % {'field': 'value', 'type': 'Object'})
    code.add_assignment(UNION_FIELD_SWITCH % {'cases': '\n      '.join(assign_cases)})

    code.copy_constructor = UNION_FIELD_SWITCH % {'cases': '\n      '.join(copy_cases)}

    code.to_string = '.add("setField", setField).add("value", value)'
    code.equals = 'Objects.equals(setField, other.setField) && Objects.equals(value, other.value)'
    code.hash_code = 'setField, value'
  else:
    builder_calls = []
    for field in struct.fields:
      generate_struct_field(code, struct, field, builder_calls)

    field_names = [f.name for f in struct.fields]
    code.copy_constructor = 'return new %s()%s;' % (struct.name, '\n        ' + '\n        '.join(builder_calls))
    code.to_string = '\n        ' + '\n        '.join(['.add("%s", %s)' % (f, f) for f in field_names])
    code.equals = '\n        && '.join(['Objects.equals(%s, other.%s)' % (f, f) for f in field_names])
    code.hash_code = '\n          ' + ',\n          '.join([f for f in field_names])

  # Special case for structs with no fields.
  if not struct.fields:
    code.equals = 'true'

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
      gen_file = os.path.join(package_dir, '%s.java' % struct.codegen_name())
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
          return thrift_type.name
      return name

    def add_param(param):
      return PARAM_METADATA_TEMPLATE % {
        'type': get_type_name(param)
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
