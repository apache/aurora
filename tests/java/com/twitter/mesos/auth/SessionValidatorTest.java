package com.twitter.mesos.auth;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;

import net.lag.crai.Crai;
import net.lag.craijce.CraiJCE;
import net.lag.jaramiko.RSAKey;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Time;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.Clock;
import com.twitter.common_internal.ldap.Ods;
import com.twitter.common_internal.ldap.User;
import com.twitter.mesos.gen.SessionKey;

import static org.easymock.EasyMock.expect;

import static com.twitter.mesos.auth.SessionValidator.SessionValidatorImpl;
import static com.twitter.mesos.auth.SessionValidator.SessionValidatorImpl.createKey;
import static com.twitter.mesos.auth.SessionValidator.UserValidator;
import static com.twitter.mesos.auth.SessionValidator.UserValidator.ODSValidator;

public class SessionValidatorTest extends EasyMockTest {

  private static final String USER = "some_user";
  private static final long NONCE = 12345;
  private static final String TOKEN = "abcdefabcdef";

  private SessionKey testKey;
  private User testUser;

  private Clock mockClock;
  private Ods mockOds;
  private UserValidator mockUserValidator;
  private UserValidator odsValidator;
  private SessionValidator sessionValidator;

  @Before
  public void setUp() throws Exception {
    testKey = createKey(USER, NONCE, TOKEN);
    testUser = new User(ImmutableMultimap.of("uid", USER));

    mockClock = createMock(Clock.class);
    mockOds = createMock(Ods.class);
    mockUserValidator = createMock(UserValidator.class);

    sessionValidator = new SessionValidatorImpl(mockClock, mockUserValidator);
    odsValidator = new ODSValidator(mockOds);
  }

  @Test(expected = SessionValidator.AuthFailedException.class)
  public void testSessionValidatorBadKey() throws Exception {
    control.replay();
    sessionValidator.checkAuthenticated(new SessionKey(), USER);
  }

  @Test(expected = SessionValidator.AuthFailedException.class)
  public void testSessionValidatorBadNonce() throws Exception {
    long finish = NONCE + 1 + SessionValidatorImpl.MAXIMUM_NONCE_DRIFT.as(Time.MILLISECONDS);
    expect(mockClock.nowMillis()).andReturn(finish);
    control.replay();
    sessionValidator.checkAuthenticated(testKey, USER);
  }

  @Test(expected = SessionValidator.AuthFailedException.class)
  public void testSessionValidatorEmptyRole() throws Exception {
    control.replay();
    sessionValidator.checkAuthenticated(testKey, "");
  }

  @Test
  public void testSessionValidatorSuccess() throws Exception {
    expect(mockClock.nowMillis()).andReturn(NONCE);
    mockUserValidator.assertRoleAccess(testKey, USER);
    control.replay();
    sessionValidator.checkAuthenticated(testKey, USER);
  }

  @Test(expected = SessionValidator.AuthFailedException.class)
  public void testOdsValidatorNoSuchUser() throws Exception {
    expect(mockOds.getUser(USER)).andReturn(null);
    control.replay();
    odsValidator.assertRoleAccess(testKey, USER);
  }

  @Test(expected = SessionValidator.AuthFailedException.class)
  public void testOdsValidatorLdapException() throws Exception {
    expect(mockOds.getUser(USER)).andThrow(new Ods.LdapException());
    control.replay();
    odsValidator.assertRoleAccess(testKey, USER);
  }

  @Test(expected = SessionValidator.AuthFailedException.class)
  public void testOdsValidatorKeyAuthFails() throws Exception {
    expect(mockOds.getUser(USER)).andReturn(testUser);
    expect(mockOds.expandKeys(USER)).andReturn(ImmutableSet.<String>of());
    control.replay();
    odsValidator.assertRoleAccess(testKey, USER);
  }

  @Test
  public void testOdsValidatorSuccess() throws Exception {
    Crai crai = new CraiJCE();
    RSAKey keyPair = RSAKey.generate(crai, 1024);
    testKey.setNonceSig(
        keyPair.signSSHData(crai, Long.toString(testKey.getNonce()).getBytes()).toByteArray());
    expect(mockOds.getUser(USER)).andReturn(testUser);
    expect(mockOds.expandKeys(USER)).andReturn(ImmutableSet.of("ssh-rsa " + keyPair.getBase64()));
    control.replay();
    odsValidator.assertRoleAccess(testKey, USER);
  }
}
