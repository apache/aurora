/**
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
package org.apache.aurora.scheduler.cron;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.BiMap;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A pattern that describes one or more cron 5-tuples (minute, hour, dayOfMonth, month, dayOfWeek).
 *
 * CrontabEntries are immutable and thread-safe. Unless otherwise specified any public methods will
 * throw {@link java.lang.NullPointerException} if given a {@code null} parameter.
 *
 * The quickest way to create a {@code CrontabEntry} is to use {@link #parse(String)} or
 * {@link #tryParse(String)}.
 */
public final class CrontabEntry {
  private static final Range<Integer> MINUTE =
      Range.closed(0, 59).canonical(DiscreteDomain.integers());
  private static final Range<Integer> HOUR =
      Range.closed(0, 23).canonical(DiscreteDomain.integers());
  private static final Range<Integer> DAY_OF_MONTH =
      Range.closed(1, 31).canonical(DiscreteDomain.integers());
  private static final Range<Integer> MONTH =
      Range.closed(1, 12).canonical(DiscreteDomain.integers());
  // NOTE: Unlike FreeBSD we don't allow "7" to mean Sunday.
  private static final Range<Integer> DAY_OF_WEEK =
      Range.closed(0, 6).canonical(DiscreteDomain.integers());

  private final RangeSet<Integer> minute;
  private final RangeSet<Integer> hour;
  private final RangeSet<Integer> dayOfMonth;
  private final RangeSet<Integer> month;
  private final RangeSet<Integer> dayOfWeek;

  private CrontabEntry(
      RangeSet<Integer> minute,
      RangeSet<Integer> hour,
      RangeSet<Integer> dayOfMonth,
      RangeSet<Integer> month,
      RangeSet<Integer> dayOfWeek) {

    checkEnclosed("minute", MINUTE, minute);
    checkEnclosed("hour", HOUR, hour);
    checkEnclosed("dayOfMonth", DAY_OF_MONTH, dayOfMonth);
    checkEnclosed("month", MONTH, month);
    checkEnclosed("dayOfWeek", DAY_OF_WEEK, dayOfWeek);

    this.minute = ImmutableRangeSet.copyOf(minute);
    this.hour = ImmutableRangeSet.copyOf(hour);
    this.dayOfMonth = ImmutableRangeSet.copyOf(dayOfMonth);
    this.month = ImmutableRangeSet.copyOf(month);
    this.dayOfWeek = ImmutableRangeSet.copyOf(dayOfWeek);

    checkArgument(hasWildcardDayOfMonth() || hasWildcardDayOfWeek(),
        "Specifying both dayOfWeek and dayOfMonth is not supported.");
  }

  private static void checkEnclosed(
      String fieldName,
      Range<Integer> fieldEnclosure,
      RangeSet<Integer> field) {

    checkArgument(fieldEnclosure.encloses(field.span()),
        String.format(
            "Bad specification for field %s: span(%s) = %s is not enclosed by boundary %s.",
            fieldName,
            field,
            field.span(),
            fieldEnclosure));
  }

  /**
   * Create a new {@link CrontabEntry} from a crontab(5)-style schedule.
   *
   * The acceptable format of {@code schedule} is mostly compatible with FreeBSD's crontab(5)
   * format, excluding "extensions" like "@every_minute."
   *
   * A crontab(5) entry consists of 5 fields (minute, hour, dayOfMonth, month, dayOfWeek) and for
   * each field supports singletons ("50"), wildcards ("*"), ranges ("1-50", "MON-SAT"), and
   * "skips" ("1-50/2", "&#42;/2").
   *
   * See http://www.freebsd.org/cgi/man.cgi?query=crontab&sektion=5 for full syntax examples.
   *
   * NOTE: While entries such as "Thursdays that fall on the 15th day of the month" are expressible
   * in the original BSD syntax, this parser rejects them with {@link IllegalArgumentException}.
   *
   * @param schedule The crontab entry to parse.
   * @return A new entry if parsing was successful.
   * @throws IllegalArgumentException if parsing failed for any reason.
   */
  public static CrontabEntry parse(String schedule) throws IllegalArgumentException {
    return new Parser(schedule).get();
  }

  /**
   * Create a new {@link CrontabEntry} from a crontab(5)-style schedule.
   *
   * @see #parse(String)
   * @param schedule The crontab entry to parse.
   * @return A new entry if parsing was successful, absent otherwise.
   */
  public static Optional<CrontabEntry> tryParse(String schedule) {
    try {
      return Optional.of(parse(schedule));
    } catch (IllegalArgumentException e) {
      return Optional.absent();
    }
  }

  private static CrontabEntry from(
      RangeSet<Integer> minute,
      RangeSet<Integer> hour,
      RangeSet<Integer> dayOfMonth,
      RangeSet<Integer> month,
      RangeSet<Integer> dayOfWeek) throws IllegalArgumentException {

    return new CrontabEntry(minute, hour, dayOfMonth, month, dayOfWeek);
  }

  private RangeSet<Integer> getMinute() {
    return minute;
  }

  private RangeSet<Integer> getHour() {
    return hour;
  }

  private RangeSet<Integer> getDayOfMonth() {
    return dayOfMonth;
  }

  private RangeSet<Integer> getMonth() {
    return month;
  }

  /**
   * The days of the week this entry matches. 0 is Sun and 6 is Sat.
   *
   * @return An immutable view of the days of the week this entry matches within [0,7).
   */
  public RangeSet<Integer> getDayOfWeek() {
    return dayOfWeek;
  }

  @VisibleForTesting
  boolean hasWildcardMinute() {
    return getMinute().encloses(MINUTE);
  }

  @VisibleForTesting
  boolean hasWildcardHour() {
    return getHour().encloses(HOUR);
  }

  /**
   * True if this entry covers all possible days of the month.
   */
  public boolean hasWildcardDayOfMonth() {
    return getDayOfMonth().encloses(DAY_OF_MONTH);
  }

  @VisibleForTesting
  boolean hasWildcardMonth() {
    return getMonth().encloses(MONTH);
  }

  /**
   * True if this entry covers all possible days of the week.
   */
  public boolean hasWildcardDayOfWeek() {
    return getDayOfWeek().encloses(DAY_OF_WEEK);
  }

  private String fieldToString(RangeSet<Integer> rangeSet, Range<Integer> coveringRange) {
    if (rangeSet.asRanges().size() == 1 && rangeSet.encloses(coveringRange)) {
      return "*";
    }
    List<String> components = Lists.newArrayList();
    for (Range<Integer> range : rangeSet.asRanges()) {
      ContiguousSet<Integer> set = ContiguousSet.create(range, DiscreteDomain.integers());
      if (set.size() == 1) {
        components.add(set.first().toString());
      } else {
        components.add(set.first() + "-" + set.last());
      }
    }
    return String.join(",", components);
  }

  /**
   * The minute component, in canonical form.
   */
  public String getMinuteAsString() {
    return fieldToString(getMinute(), MINUTE);
  }

  /**
   * The hour component, in canonical form.
   */
  public String getHourAsString() {
    return fieldToString(getHour(), HOUR);
  }

  /**
   * The dayOfMonth component, in canonical form.
   */
  public String getDayOfMonthAsString() {
    return fieldToString(getDayOfMonth(), DAY_OF_MONTH);
  }

  /**
   * The month component, in canonical form.
   */
  public String getMonthAsString() {
    return fieldToString(getMonth(), MONTH);
  }

  /**
   * The dayOfWeek component, in canonical form.
   */
  public String getDayOfWeekAsString() {
    return fieldToString(getDayOfWeek(), DAY_OF_WEEK);
  }

  /**
   * Returns a parsable string representation schedule such that
   * {@code c.equals(CrontabEntry.parse(c.toString())}.
   */
  @Override
  public String toString() {
    return String.join(
        " ",
        getMinuteAsString(),
        getHourAsString(),
        getDayOfMonthAsString(),
        getMonthAsString(),
        getDayOfWeekAsString());
  }

  /**
   * True when both sides would match the same set of instants.
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CrontabEntry)) {
      return false;
    }
    CrontabEntry that = (CrontabEntry) o;
    return Objects.equals(getMinute(), that.getMinute())
        && Objects.equals(getHour(), that.getHour())
        && Objects.equals(getDayOfMonth(), that.getDayOfMonth())
        && Objects.equals(getMonth(), that.getMonth())
        && Objects.equals(getDayOfWeek(), that.getDayOfWeek());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMinute(), getHour(), getDayOfWeek(), getMonth(), getDayOfMonth());
  }

  private static class Parser {
    private static final Pattern CRONTAB_ENTRY = Pattern.compile(
        "^(?<minute>\\S+)"
            + "\\s+(?<hour>\\S+)"
            + "\\s+(?<dayOfMonth>\\S+)"
            + "\\s+(?<month>\\S+)"
            + "\\s+(?<dayOfWeek>\\S+)$"
    );

    // A single time like "5", "10", "50".
    private static final Pattern NUMBER = Pattern.compile("^(?<number>\\d+)$");
    // A wildcard ("*").
    private static final Pattern WILDCARD = Pattern.compile("^\\*$");
    // A range like "1-2", "5-10", "14-50".
    private static final Pattern RANGE = Pattern.compile("^(?<lower>\\d+)-(?<upper>\\d+)$");
    // A wildcard with a "skip" like "*/5", "*/10"
    private static final Pattern WILDCARD_WITH_SKIP = Pattern.compile("^\\*/(?<skip>\\d+)$");
    // A range with a "skip" like "1-2/2", "0-59/5"
    private static final Pattern RANGE_WITH_SKIP =
        Pattern.compile("^(?<lower>\\d+)-(?<upper>\\d+)/(?<skip>\\d+)$");

    private static final BiMap<String, Integer> MONTH_NAMES = ImmutableBiMap
        .<String, Integer>builder()
        .put("JAN", 1)
        .put("FEB", 2)
        .put("MAR", 3)
        .put("APR", 4)
        .put("MAY", 5)
        .put("JUN", 6)
        .put("JUL", 7)
        .put("AUG", 8)
        .put("SEP", 9)
        .put("OCT", 10)
        .put("NOV", 11)
        .put("DEC", 12)
        .build();

    // NOTE: Unlike FreeBSD we don't allow "7" to mean Sunday.
    private static final BiMap<String, Integer> DAY_NAMES = ImmutableBiMap
        .<String, Integer>builder()
        .put("SUN", 0)
        .put("MON", 1)
        .put("TUE", 2)
        .put("WED", 3)
        .put("THU", 4)
        .put("FRI", 5)
        .put("SAT", 6)
        .build();

    private final String rawMinute;
    private final String rawHour;
    private final String rawDayOfMonth;
    private final String rawMonth;
    private final String rawDayOfWeek;

    Parser(String schedule) throws IllegalArgumentException {
      Matcher matcher = CRONTAB_ENTRY.matcher(schedule);
      checkArgument(matcher.matches(), "Invalid cron schedule %s", schedule);

      rawMinute = requireNonNull(matcher.group("minute"));
      rawHour = requireNonNull(matcher.group("hour"));
      rawDayOfMonth = requireNonNull(matcher.group("dayOfMonth"));
      rawMonth = requireNonNull(matcher.group("month"));
      rawDayOfWeek = requireNonNull(matcher.group("dayOfWeek"));
    }

    CrontabEntry get() throws IllegalArgumentException {
      return CrontabEntry.from(
          parseMinute(),
          parseHour(),
          parseDayOfMonth(),
          parseMonth(),
          parseDayOfWeek());
    }

    private List<String> getComponents(String rawField) {
      return Splitter.on(",").omitEmptyStrings().splitToList(rawField);
    }

    private String replaceNameAliases(String rawComponent, Map<String, Integer> names) {
      String component = rawComponent.toUpperCase(Locale.ENGLISH);
      for (Map.Entry<String, Integer> entry : names.entrySet()) {
        if (component.contains(entry.getKey())) {
          component = component.replaceAll(entry.getKey(), entry.getValue().toString());
        }
      }
      return component;
    }

    private static RangeSet<Integer> parseComponent(
        final Range<Integer> enclosure,
        String rawComponent) throws IllegalArgumentException {

      if (WILDCARD.matcher(rawComponent).matches()) {
        return ImmutableRangeSet.of(enclosure);
      }

      Matcher matcher = NUMBER.matcher(rawComponent);
      if (matcher.matches()) {
        int number = Integer.parseInt(matcher.group("number"));
        Range<Integer> range = Range.singleton(number).canonical(DiscreteDomain.integers());

        checkArgument(enclosure.encloses(range), "%s does not enclose %s", enclosure, range);

        return ImmutableRangeSet.of(range);
      }

      matcher = RANGE.matcher(rawComponent);
      if (matcher.matches()) {
        int lower = Integer.parseInt(matcher.group("lower"));
        int upper = Integer.parseInt(matcher.group("upper"));
        Range<Integer> range = Range.closed(lower, upper).canonical(DiscreteDomain.integers());

        checkArgument(enclosure.encloses(range), "%s does not enclose %s", enclosure, range);

        return ImmutableRangeSet.of(range);
      }

      matcher = WILDCARD_WITH_SKIP.matcher(rawComponent);
      if (matcher.matches()) {
        int skip = Integer.parseInt(matcher.group("skip"));
        int start = enclosure.lowerEndpoint();

        checkArgument(skip > 0);

        ImmutableRangeSet.Builder<Integer> rangeSet = ImmutableRangeSet.builder();
        for (int i = start;  enclosure.contains(i); i += skip) {
          rangeSet.add(Range.singleton(i).canonical(DiscreteDomain.integers()));
        }
        return rangeSet.build();
      }

      matcher = RANGE_WITH_SKIP.matcher(rawComponent);
      if (matcher.matches()) {
        final int lower = Integer.parseInt(matcher.group("lower"));
        final int upper = Integer.parseInt(matcher.group("upper"));
        final int skip = Integer.parseInt(matcher.group("skip"));
        Range<Integer> range = Range.closed(lower, upper).canonical(DiscreteDomain.integers());

        checkArgument(enclosure.encloses(range), "%s does not enclose %s", enclosure, range);
        checkArgument(skip > 0, "skip value %s must be >0", skip);
        checkArgument(skip < upper, "skip value %s must be smaller than %s", skip, upper);

        ImmutableRangeSet.Builder<Integer> rangeSet = ImmutableRangeSet.builder();
        for (int i = lower; range.contains(i); i += skip) {
          rangeSet.add(Range.singleton(i).canonical(DiscreteDomain.integers()));
        }
        return rangeSet.build();
      }

      throw new IllegalArgumentException(
          "Cron schedule component " + rawComponent + " does not match any known patterns.");
    }

    private RangeSet<Integer> parseMinute() {
      RangeSet<Integer> minutes = TreeRangeSet.create();
      for (String component : getComponents(rawMinute)) {
        minutes.addAll(parseComponent(MINUTE, component));
      }
      return ImmutableRangeSet.copyOf(minutes);
    }

    private RangeSet<Integer> parseHour() {
      RangeSet<Integer> hours = TreeRangeSet.create();
      for (String component : getComponents(rawHour)) {
        hours.addAll(parseComponent(HOUR, component));
      }
      return ImmutableRangeSet.copyOf(hours);
    }

    private RangeSet<Integer> parseDayOfWeek() {
      RangeSet<Integer> daysOfWeek = TreeRangeSet.create();
      for (String component : getComponents(rawDayOfWeek)) {
        daysOfWeek.addAll(parseComponent(DAY_OF_WEEK, replaceNameAliases(component, DAY_NAMES)));
      }
      return ImmutableRangeSet.copyOf(daysOfWeek);
    }

    private RangeSet<Integer> parseMonth() {
      RangeSet<Integer> months = TreeRangeSet.create();
      for (String component : getComponents(rawMonth)) {
        months.addAll(parseComponent(MONTH, replaceNameAliases(component, MONTH_NAMES)));
      }
      return ImmutableRangeSet.copyOf(months);
    }

    private RangeSet<Integer> parseDayOfMonth() {
      RangeSet<Integer> daysOfMonth = TreeRangeSet.create();
      for (String component : getComponents(rawDayOfMonth)) {
        daysOfMonth.addAll(parseComponent(DAY_OF_MONTH, component));
      }
      return ImmutableRangeSet.copyOf(daysOfMonth);
    }
  }
}
