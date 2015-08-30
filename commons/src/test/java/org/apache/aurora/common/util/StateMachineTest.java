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
package org.apache.aurora.common.util;

import org.apache.aurora.common.base.Closure;
import org.apache.aurora.common.base.Closures;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.StateMachine.Rule;
import org.apache.aurora.common.util.StateMachine.Transition;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests the functionality of StateMachine.
 *
 * @author William Farner
 */
public class StateMachineTest extends EasyMockTest {
  private static final String NAME = "State machine.";

  private static final String A = "A";
  private static final String B = "B";
  private static final String C = "C";
  private static final String D = "D";

  @Test
  public void testEmptySM() {
    control.replay();

    try {
      StateMachine.builder(NAME).build();
      fail();
    } catch (IllegalStateException e) {
      // Expected.
    }
  }

  @Test
  public void testMachineNoInit() {
    control.replay();

    try {
      StateMachine.<String>builder(NAME)
          .addState(Rule.from(A).to(B))
          .build();
      fail();
    } catch (IllegalStateException e) {
      // Expected.
    }
  }

  @Test
  public void testBasicFSM() {
    control.replay();

    StateMachine<String> fsm = StateMachine.<String>builder(NAME)
        .initialState(A)
        .addState(Rule.from(A).to(B))
        .addState(Rule.from(B).to(C))
        .addState(Rule.from(C).to(D))
        .build();

    assertThat(fsm.getState(), is(A));
    changeState(fsm, B);
    changeState(fsm, C);
    changeState(fsm, D);
  }

  @Test
  public void testLoopingFSM() {
    control.replay();

    StateMachine<String> fsm = StateMachine.<String>builder(NAME)
        .initialState(A)
        .addState(Rule.from(A).to(B))
        .addState(Rule.from(B).to(C))
        .addState(Rule.from(C).to(B, D))
        .build();

    assertThat(fsm.getState(), is(A));
    changeState(fsm, B);
    changeState(fsm, C);
    changeState(fsm, B);
    changeState(fsm, C);
    changeState(fsm, B);
    changeState(fsm, C);
    changeState(fsm, D);
  }

  @Test
  public void testMachineUnknownState() {
    control.replay();

    StateMachine<String> fsm = StateMachine.<String>builder(NAME)
        .initialState(A)
        .addState(Rule.from(A).to(B))
        .addState(Rule.from(B).to(C))
        .build();

    assertThat(fsm.getState(), is(A));
    changeState(fsm, B);
    changeState(fsm, C);
    changeStateFail(fsm, D);
  }

  @Test
  public void testMachineBadTransition() {
    control.replay();

    StateMachine<String> fsm = StateMachine.<String>builder(NAME)
        .initialState(A)
        .addState(Rule.from(A).to(B))
        .addState(Rule.from(B).to(C))
        .build();

    assertThat(fsm.getState(), is(A));
    changeState(fsm, B);
    changeState(fsm, C);
    changeStateFail(fsm, B);
  }

  @Test
  public void testMachineSelfTransitionAllowed() {
    control.replay();

    StateMachine<String> fsm = StateMachine.<String>builder(NAME)
        .initialState(A)
        .addState(Rule.from(A).to(A))
        .build();

    assertThat(fsm.getState(), is(A));
    changeState(fsm, A);
    changeState(fsm, A);
  }

  @Test
  public void testMachineSelfTransitionDisallowed() {
    control.replay();

    StateMachine<String> fsm = StateMachine.<String>builder(NAME)
        .initialState(A)
        .addState(Rule.from(A).to(B))
        .build();

    assertThat(fsm.getState(), is(A));
    changeStateFail(fsm, A);
    changeStateFail(fsm, A);
  }

  @Test
  public void testCheckStateMatches() {
    control.replay();

    StateMachine<String> stateMachine = StateMachine.<String>builder(NAME)
        .initialState(A)
        .addState(Rule.from(A).to(B))
        .build();
    stateMachine.checkState(A);
    stateMachine.transition(B);
    stateMachine.checkState(B);
  }

  @Test(expected = IllegalStateException.class)
  public void testCheckStateFails() {
    control.replay();

    StateMachine.<String>builder(NAME)
        .initialState(A)
        .addState(Rule.from(A).to(B))
        .build()
        .checkState(B);
  }

  @Test
  public void testNoThrowOnInvalidTransition() {
    control.replay();

    StateMachine<String> machine = StateMachine.<String>builder(NAME)
        .initialState(A)
        .addState(A, B)
        .throwOnBadTransition(false)
        .build();

    machine.transition(C);
    assertThat(machine.getState(), is(A));
  }

  private static final Clazz<Closure<Transition<String>>> TRANSITION_CLOSURE_CLZ =
      new Clazz<Closure<Transition<String>>>() {};

  @Test
  public void testTransitionCallbacks() {
    Closure<Transition<String>> anyTransition = createMock(TRANSITION_CLOSURE_CLZ);
    Closure<Transition<String>> fromA = createMock(TRANSITION_CLOSURE_CLZ);
    Closure<Transition<String>> fromB = createMock(TRANSITION_CLOSURE_CLZ);

    Transition<String> aToB = new Transition<>(A, B, true);
    anyTransition.execute(aToB);
    fromA.execute(aToB);

    Transition<String> bToB = new Transition<>(B, B, false);
    anyTransition.execute(bToB);
    fromB.execute(bToB);

    Transition<String> bToC = new Transition<>(B, C, true);
    anyTransition.execute(bToC);
    fromB.execute(bToC);

    anyTransition.execute(new Transition<>(C, B, true));

    Transition<String> bToD = new Transition<>(B, D, true);
    anyTransition.execute(bToD);
    fromB.execute(bToD);

    control.replay();

    StateMachine<String> machine = StateMachine.<String>builder(NAME)
        .initialState(A)
        .addState(Rule.from(A).to(B).withCallback(fromA))
        .addState(Rule.from(B).to(C, D).withCallback(fromB))
        .addState(Rule.from(C).to(B))
        .addState(Rule.from(D).noTransitions())
        .onAnyTransition(anyTransition)
        .throwOnBadTransition(false)
        .build();

    machine.transition(B);
    machine.transition(B);
    machine.transition(C);
    machine.transition(B);
    machine.transition(D);
  }

  @Test
  public void testFilteredTransitionCallbacks() {
    Closure<Transition<String>> aToBHandler = createMock(TRANSITION_CLOSURE_CLZ);
    Closure<Transition<String>> impossibleHandler = createMock(TRANSITION_CLOSURE_CLZ);

    aToBHandler.execute(new Transition<>(A, B, true));

    control.replay();

    StateMachine<String> machine = StateMachine.<String>builder(NAME)
        .initialState(A)
        .addState(Rule
            .from(A).to(B, C)
            .withCallback(Closures.filter(Transition.to(B), aToBHandler)))
        .addState(Rule.from(B).to(A)
            .withCallback(Closures.filter(Transition.to(B), impossibleHandler)))
        .addState(Rule.from(C).noTransitions())
        .build();

    machine.transition(B);
    machine.transition(A);
    machine.transition(C);
  }

  private static void changeState(StateMachine<String> machine, String to, boolean expectAllowed) {
    boolean allowed = true;
    try {
      machine.transition(to);
      assertThat(machine.getState(), is(to));
    } catch (StateMachine.IllegalStateTransitionException e) {
      allowed = false;
    }

    assertThat(allowed, is(expectAllowed));
  }

  private static void changeState(StateMachine<String> machine, String to) {
    changeState(machine, to, true);
  }

  private static void changeStateFail(StateMachine<String> machine, String to) {
    changeState(machine, to, false);
  }
}
