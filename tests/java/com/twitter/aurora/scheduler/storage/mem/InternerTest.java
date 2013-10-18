/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler.storage.mem;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class InternerTest {

  private static final Internable JOAN = new Internable("joan");
  private static final Internable SAME_JOAN = new Internable("joan");
  private static final Internable STEVE = new Internable("steve");

  private static final String BOB = "bob";
  private static final String BOB2 = "bob2";
  private static final String BARRY = "barry";

  private Interner<Internable, String> interner;

  @Before
  public void setUp() {
    interner = new Interner<Internable, String>();
  }

  @Test
  public void testReferenceCounting() {
    assertSame(JOAN, interner.addAssociation(JOAN, BOB));
    assertSame(JOAN, interner.addAssociation(SAME_JOAN, BOB2));
    assertEquals(ImmutableSet.of(BOB2, BOB), interner.getAssociations(JOAN));
    assertTrue(interner.isInterned(JOAN));
    assertTrue(interner.isInterned(SAME_JOAN));

    // Re-associate.
    assertSame(JOAN, interner.addAssociation(JOAN, BOB));
    assertEquals(ImmutableSet.of(BOB2, BOB), interner.getAssociations(JOAN));

    // Noop.
    interner.removeAssociation(JOAN, "bob3");
    assertEquals(ImmutableSet.of(BOB2, BOB), interner.getAssociations(JOAN));

    interner.removeAssociation(JOAN, BOB);
    assertEquals(ImmutableSet.of(BOB2), interner.getAssociations(JOAN));

    interner.removeAssociation(JOAN, BOB2);
    assertFalse(interner.isInterned(JOAN));
  }

  @Test
  public void testNonEqual() {
    assertSame(JOAN, interner.addAssociation(JOAN, BOB));
    assertSame(STEVE, interner.addAssociation(STEVE, BOB));
    assertSame(JOAN, interner.addAssociation(JOAN, BOB));
    assertSame(STEVE, interner.addAssociation(STEVE, BOB));
    assertSame(STEVE, interner.addAssociation(STEVE, BARRY));
    assertEquals(ImmutableSet.of(BOB), interner.getAssociations(JOAN));
    assertEquals(ImmutableSet.of(BARRY, BOB), interner.getAssociations(STEVE));

    interner.removeAssociation(JOAN, BOB);
    assertFalse(interner.isInterned(JOAN));

    interner.removeAssociation(STEVE, BOB);
    assertEquals(ImmutableSet.of(BARRY), interner.getAssociations(STEVE));
    interner.removeAssociation(STEVE, BARRY);
    assertFalse(interner.isInterned(STEVE));
  }

  @Test
  public void testNoopRemoveAssociation() {
    interner.removeAssociation(JOAN, BOB);
    assertFalse(interner.isInterned(JOAN));
  }

  @Test
  public void testClear() {
    assertSame(JOAN, interner.addAssociation(JOAN, BOB));

    interner.clear();
    assertFalse(interner.isInterned(JOAN));
  }

  private static class Internable {
    private final String value;

    Internable(String value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Internable)) {
        return false;
      }
      Internable other = (Internable) o;
      return this.value.equals(other.value);
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }
}
