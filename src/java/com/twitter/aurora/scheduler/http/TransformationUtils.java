package com.twitter.aurora.scheduler.http;

import java.util.Collection;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;

import com.twitter.aurora.gen.Package;
import com.twitter.aurora.scheduler.base.Numbers;

/**
 * Utility class to hold common object to string transformation helper functions.
 */
final class TransformationUtils {
  public static final Function<Package, String> PACKAGE_TOSTRING =
      new Function<Package, String>() {
        @Override public String apply(Package pkg) {
          return pkg.getRole() + "/" + pkg.getName() + " v" + pkg.getVersion();
        }
      };

  public static final Function<Range<Integer>, String> RANGE_TOSTRING =
      new Function<Range<Integer>, String>() {
        @Override public String apply(Range<Integer> range) {
          int lower = range.lowerEndpoint();
          int upper = range.upperEndpoint();
          return (lower == upper) ? String.valueOf(lower) : (lower + " - " + upper);
        }
      };

  public static final Function<Collection<Integer>, String> SHARDS_TOSTRING =
      new Function<Collection<Integer>, String>() {
        @Override public String apply(Collection<Integer> shards) {
          return Joiner.on(", ")
              .join(Iterables.transform(Numbers.toRanges(shards), RANGE_TOSTRING));
        }
      };

  private TransformationUtils() {
    // Utility class
  }
}
