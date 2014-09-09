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

#
# Code to compute field-wise tree diffs over json trees, with exclusions.
#

from collections import namedtuple

from twitter.common.lang import Compatibility


def prune_structure(struct, exclusions):
  """Remove fields specified in an exclusion from a json dictionary."""
  result = dict(struct)
  for e in exclusions:
    if isinstance(e, list) and len(e) == 1 and isinstance(e[0], Compatibility.string):
      e = e[0]
    if isinstance(e, Compatibility.string) and e in struct:
      del result[e]
    else:
      first = e[0]
      if first in struct:
        result[first] = prune_structure(result[first], [e[1:]])
  return result


FieldDifference = namedtuple("FieldDifference", ["name", "base", "other"])


# JSON sets are converted to JSON as lists, but the order of elements in the list
# isn't consistent - it's possible to list-ify the elements of two identical sets,
# and get lists with the elements in different orders. So we need to fix that.
#
# For the configs that we'll be comparing with this code, all of the collections
# are lists, which means that we can just sort the elements of any list, and then
# do a list comparison.
#
# But for the future, we should really find a better solution, like fixing the
# code that converts thrift to JSON in order to make it generate sets consistently.
def canonicalize_json(val):
  def canonicalize_list(lst):
    result = []
    for l in lst:
      result.append(canonicalize_json(l))
    result.sort()
    return tuple(result)

  def canonicalize_dict(dct):
    result = {}
    for key in dct:
      result[key] = canonicalize_json(dct[key])
    return result

  if isinstance(val, list):
    return canonicalize_list(val)
  elif isinstance(val, dict):
    return canonicalize_dict(val)
  elif isinstance(val, set):
    return canonicalize_list(set)
  else:
    return val


def compare_json(base, other, path):
  """Do a field-wise comparison between two json trees.
  :param base: one of the JSON trees to compare
  :param other: the other JSON tree
  :param path: a list of string describing the path to the trees being
      compared (used for labelling the diff.)"""

  keys = set(base.keys()) | set(other.keys())
  differences = []
  for key in keys:
    base_val = canonicalize_json(base.get(key, "__undefined__"))
    other_val = canonicalize_json(other.get(key, "__undefined__"))
    if base_val != other_val:
      if isinstance(base_val, dict) and isinstance(other_val, dict):
        differences += compare_json(base_val, other_val, path + [key])
      else:
        differences += [FieldDifference('.'.join(path + [key]), base_val, other_val)]
  return differences


def compare_pruned_json(base, other, excludes):
  """Compares two thrift objects, which have been rendered as JSON dictionaries.

  The two are considered equal if the fields outside of the excludes list are
  equal.
  :param base: one version of the thrift object; assumed to be the original, unmodified structure.
  :param other: a second version of the thrift object, assumed to be a possibly modified copy
       of the base.
  :param excludes: a structured list of fields that should not be considered in the comparison.
      Each element is either a string or a list.
       - If an entry is a string, then it is the name of a field in the object whose value
         should not be considered in the comparison.
       - If an entry is a list [x, y, z], then it is interpreted as a path specification
         for a field in a nested object. The list [x, y, z] would mean "ignore the field
         z of the object that is in the field y of the object in the field x of the two
         objects being compared."
         ["x"] and "x" are always equivalent.
  """
  pruned_base = prune_structure(base, excludes)
  pruned_other = prune_structure(other, excludes)
  return compare_json(pruned_base, pruned_other, [])
