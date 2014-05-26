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
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CrontabEntryTest {
  @Test
  public void testHashCodeAndEquals() {
    List<CrontabEntry> entries = ImmutableList.of(
        CrontabEntry.parse("* * * * *"),
        CrontabEntry.parse("0-59 * * * *"),
        CrontabEntry.parse("0-57,58,59 * * * *"),
        CrontabEntry.parse("* 23,1,2,4,0-22 * * *"),
        CrontabEntry.parse("1-50,0,51-59 * * * sun-sat"));

    for (CrontabEntry lhs : entries) {
      for (CrontabEntry rhs : entries) {
        assertEquals(lhs, rhs);
      }
    }

    Set<CrontabEntry> equivalentEntries = Sets.newHashSet(entries);
    assertTrue(equivalentEntries.size() == 1);
  }

  @Test
  public void testEqualsCoverage() {
    assertNotEquals(CrontabEntry.parse("* * * * *"), new Object());

    assertNotEquals(CrontabEntry.parse("* * * * *"), CrontabEntry.parse("1 * * * *"));
    assertEquals(CrontabEntry.parse("1,2,3 * * * *"), CrontabEntry.parse("1-3 * * * *"));

    assertNotEquals(CrontabEntry.parse("* 0-22 * * *"), CrontabEntry.parse("* * * * *"));
    assertEquals(CrontabEntry.parse("* 0-23 * * *"), CrontabEntry.parse("* * * * *"));

    assertNotEquals(CrontabEntry.parse("1 1 1-30 * *"), CrontabEntry.parse("1 1 * * *"));
    assertEquals(CrontabEntry.parse("1 1 1-31 * *"), CrontabEntry.parse("1 1 * * *"));

    assertNotEquals(CrontabEntry.parse("1 1 * JAN,FEB-NOV *"), CrontabEntry.parse("1 1 * * *"));
    assertEquals(CrontabEntry.parse("1 1 * JAN,FEB-DEC *"), CrontabEntry.parse("1 1 * * *"));

    assertNotEquals(CrontabEntry.parse("* * * * SUN"), CrontabEntry.parse("* * * * SAT"));
    assertEquals(CrontabEntry.parse("* * * * 0"), CrontabEntry.parse("* * * * SUN"));
  }

  @Test
  public void testSkip() {
    assertEquals(CrontabEntry.parse("*/15 * * * *"), CrontabEntry.parse("0,15,30,45 * * * *"));
    assertEquals(
        CrontabEntry.parse("* */2 * * *"),
        CrontabEntry.parse("0-59 0,2,4,6,8,10,12-23/2  * * *"));
  }

  @Test
  public void testToString() {
    assertEquals("0-58 * * * *", CrontabEntry.parse("0,1-57,58 * * * *").toString());
    assertEquals("* * * * *", CrontabEntry.parse("* * * * *").toString());
  }

  @Test
  public void testWildcards() {
    CrontabEntry wildcardMinuteEntry = CrontabEntry.parse("* 1 1 1 *");
    assertEquals("*", wildcardMinuteEntry.getMinuteAsString());
    assertTrue(wildcardMinuteEntry.hasWildcardMinute());
    assertFalse(wildcardMinuteEntry.hasWildcardHour());
    assertFalse(wildcardMinuteEntry.hasWildcardDayOfMonth());
    assertFalse(wildcardMinuteEntry.hasWildcardMonth());
    assertTrue(wildcardMinuteEntry.hasWildcardDayOfWeek());

    CrontabEntry wildcardHourEntry = CrontabEntry.parse("1 * 1 1 *");
    assertEquals("*", wildcardHourEntry.getHourAsString());
    assertFalse(wildcardHourEntry.hasWildcardMinute());
    assertTrue(wildcardHourEntry.hasWildcardHour());
    assertFalse(wildcardHourEntry.hasWildcardDayOfMonth());
    assertFalse(wildcardHourEntry.hasWildcardMonth());
    assertTrue(wildcardHourEntry.hasWildcardDayOfWeek());

    CrontabEntry wildcardDayOfMonth = CrontabEntry.parse("1 1 * 1 *");
    assertEquals("*", wildcardDayOfMonth.getDayOfMonthAsString());
    assertFalse(wildcardDayOfMonth.hasWildcardMinute());
    assertFalse(wildcardDayOfMonth.hasWildcardHour());
    assertTrue(wildcardDayOfMonth.hasWildcardDayOfMonth());
    assertFalse(wildcardDayOfMonth.hasWildcardMonth());
    assertTrue(wildcardDayOfMonth.hasWildcardDayOfWeek());

    CrontabEntry wildcardMonth = CrontabEntry.parse("1 1 1 * *");
    assertEquals("*", wildcardMonth.getMonthAsString());
    assertFalse(wildcardMonth.hasWildcardMinute());
    assertFalse(wildcardMonth.hasWildcardHour());
    assertFalse(wildcardMonth.hasWildcardDayOfMonth());
    assertTrue(wildcardMonth.hasWildcardMonth());
    assertTrue(wildcardMonth.hasWildcardDayOfWeek());

    CrontabEntry wildcardDayOfWeek = CrontabEntry.parse("1 1 1 1 *");
    assertEquals("*", wildcardDayOfWeek.getDayOfWeekAsString());
    assertFalse(wildcardDayOfWeek.hasWildcardMinute());
    assertFalse(wildcardDayOfWeek.hasWildcardHour());
    assertFalse(wildcardDayOfWeek.hasWildcardDayOfMonth());
    assertFalse(wildcardDayOfWeek.hasWildcardMonth());
    assertTrue(wildcardDayOfWeek.hasWildcardDayOfWeek());
  }

  @Test
  public void testEqualsIsCanonical() {
    String rawEntry = "* * */3 * *";
    CrontabEntry input = CrontabEntry.parse(rawEntry);
    assertNotEquals(
        rawEntry + " is not the canonical form of " + input,
        rawEntry,
        input.toString());
    assertEquals(
        "The form returned by toString is canonical",
        input.toString(),
        CrontabEntry.parse(input.toString()).toString());
  }

  @Test
  public void testBadEntries() {
    List<String> badPatterns = ImmutableList.of(
        "* * * * MON-SUN",
        "* * **",
        "0-59 0-59 * * *",
        "1/1 * * * *",
        "5 5 * MAR-JAN *",
        "*/0 * * * *",
        "0-59/0 * * * *",
        "0-59/60 * * * *",
        "* * 1 * 1"
    );

    for (String pattern : badPatterns) {
      assertNull(CrontabEntry.tryParse(pattern).orNull());
    }
  }

  @Test
  public void testExpectedTriggerPredictionsParse() {
    for (ExpectedPrediction prediction : ExpectedPrediction.getAll()) {
      prediction.parseCrontabEntry();
    }
  }
}
