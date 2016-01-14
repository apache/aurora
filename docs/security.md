Aurora integrates with [Apache Shiro](http://shiro.apache.org/) to provide security
controls for its API. In addition to providing some useful features out of the box, Shiro
also allows Aurora cluster administrators to adapt the security system to their organization’s
existing infrastructure.

- [Enabling Security](#enabling-security)
- [Authentication](#authentication)
	- [HTTP Basic Authentication](#http-basic-authentication)
		- [Server Configuration](#server-configuration)
		- [Client Configuration](#client-configuration)
	- [HTTP SPNEGO Authentication (Kerberos)](#http-spnego-authentication-kerberos)
		- [Server Configuration](#server-configuration-1)
		- [Client Configuration](#client-configuration-1)
- [Authorization](#authorization)
	- [Using an INI file to define security controls](#using-an-ini-file-to-define-security-controls)
		- [Caveats](#caveats)
- [Implementing a Custom Realm](#implementing-a-custom-realm)
	- [Packaging a realm module](#packaging-a-realm-module)
- [Known Issues](#known-issues)

# Enabling Security

There are two major components of security:
[authentication and authorization](http://en.wikipedia.org/wiki/Authentication#Authorization).  A
cluster administrator may choose the approach used for each, and may also implement custom
mechanisms for either.  Later sections describe the options available.

# Authentication

The scheduler must be configured with instructions for how to process authentication
credentials at a minimum.  There are currently two built-in authentication schemes -
[HTTP Basic Authentication](http://en.wikipedia.org/wiki/Basic_access_authentication), and
[SPNEGO](http://en.wikipedia.org/wiki/SPNEGO) (Kerberos).

## HTTP Basic Authentication

Basic Authentication is a very quick way to add *some* security.  It is supported
by all major browsers and HTTP client libraries with minimal work.  However,
before relying on Basic Authentication you should be aware of the [security
considerations](http://tools.ietf.org/html/rfc2617#section-4).

### Server Configuration

At a minimum you need to set 4 command-line flags on the scheduler:

```
-http_authentication_mechanism=BASIC
-shiro_realm_modules=INI_AUTHNZ
-shiro_ini_path=path/to/security.ini
```

And create a security.ini file like so:

```
[users]
sally = apple, admin

[roles]
admin = *
```

The details of the security.ini file are explained below. Note that this file contains plaintext,
unhashed passwords.

### Client Configuration

To configure the client for HTTP Basic authentication, add an entry to ~/.netrc with your credentials

```
% cat ~/.netrc
# ...

machine aurora.example.com
login sally
password apple

# ...
```

No changes are required to `clusters.json`.

## HTTP SPNEGO Authentication (Kerberos)

### Server Configuration
At a minimum you need to set 6 command-line flags on the scheduler:

```
-http_authentication_mechanism=NEGOTIATE
-shiro_realm_modules=KERBEROS5_AUTHN,INI_AUTHNZ
-kerberos_server_principal=HTTP/aurora.example.com@EXAMPLE.COM
-kerberos_server_keytab=path/to/aurora.example.com.keytab
-shiro_ini_path=path/to/security.ini
```

And create a security.ini file like so:

```
% cat path/to/security.ini
[users]
sally = _, admin

[roles]
admin = *
```

What's going on here? First, Aurora must be configured to request Kerberos credentials when presented with an
unauthenticated request. This is achieved by setting

```
-http_authentication_mechanism=NEGOTIATE
```

Next, a Realm module must be configured to **authenticate** the current request using the Kerberos
credentials that were requested. Aurora ships with a realm module that can do this

```
-shiro_realm_modules=KERBEROS5_AUTHN[,...]
```

The Kerberos5Realm requires a keytab file and a server principal name. The principal name will usually
be in the form `HTTP/aurora.example.com@EXAMPLE.COM`.

```
-kerberos_server_principal=HTTP/aurora.example.com@EXAMPLE.COM
-kerberos_server_keytab=path/to/aurora.example.com.keytab
```

The Kerberos5 realm module is authentication-only. For scheduler security to work you must also
enable a realm module that provides an Authorizer implementation. For example, to do this using the
IniShiroRealmModule:

```
-shiro_realm_modules=KERBEROS5_AUTHN,INI_AUTHNZ
```

You can then configure authorization using a security.ini file as described below
(the password field is ignored). You must configure the realm module with the path to this file:

```
-shiro_ini_path=path/to/security.ini
```

### Client Configuration
To use Kerberos on the client-side you must build Kerberos-enabled client binaries. Do this with

```
./pants binary src/main/python/apache/aurora/kerberos:kaurora
./pants binary src/main/python/apache/aurora/kerberos:kaurora_admin
```

You must also configure each cluster where you've enabled Kerberos on the scheduler
to use Kerberos authentication. Do this by setting `auth_mechanism` to `KERBEROS`
in `clusters.json`.

```
% cat ~/.aurora/clusters.json
{
    "devcluser": {
        "auth_mechanism": "KERBEROS",
        ...
    },
    ...
}
```

# Authorization
Given a means to authenticate the entity a client claims they are, we need to define what privileges they have.

## Using an INI file to define security controls

The simplest security configuration for Aurora is an INI file on the scheduler.  For small
clusters, or clusters where the users and access controls change relatively infrequently, this is
likely the preferred approach.  However you may want to avoid this approach if access permissions
are rapidly changing, or if your access control information already exists in another system.

You can enable INI-based configuration with following scheduler command line arguments:

```
-http_authentication_mechanism=BASIC
-shiro_ini_path=path/to/security.ini
```

*note* As the argument name reveals, this is using Shiro’s
[IniRealm](http://shiro.apache.org/configuration.html#Configuration-INIConfiguration) behind
the scenes.

The INI file will contain two sections - users and roles.  Here’s an example for what might
be in security.ini:

```
[users]
sally = apple, admin
jim = 123456, accounting
becky = letmein, webapp
larry = 654321,accounting
steve = password

[roles]
admin = *
accounting = thrift.AuroraAdmin:setQuota
webapp = thrift.AuroraSchedulerManager:*:webapp
```

The users section defines user user credentials and the role(s) they are members of.  These lines
are of the format `<user> = <password>[, <role>...]`.  As you probably noticed, the passwords are
in plaintext and as a result read access to this file should be restricted.

In this configuration, each user has different privileges for actions in the cluster because
of the roles they are a part of:

* admin is granted all privileges
* accounting may adjust the amount of resource quota for any role
* webapp represents a collection of jobs that represents a service, and its members may create and modify any jobs owned by it

### Caveats
You might find documentation on the Internet suggesting there are additional sections in `shiro.ini`,
like `[main]` and `[urls]`. These are not supported by Aurora as it uses a different mechanism to configure
those parts of Shiro. Think of Aurora's `security.ini` as a subset with only `[users]` and `[roles]` sections.

## Implementing Delegated Authorization

It is possible to leverage Shiro's `runAs` feature by implementing a custom Servlet Filter that provides
the capability and passing it's fully qualified class name to the command line argument
`-shiro_after_auth_filter`. The filter is registered in the same filter chain as the Shiro auth filters
and is placed after the Shiro auth filters in the filter chain. This ensures that the Filter is invoked
after the Shiro filters have had a chance to authenticate the request.

# Implementing a Custom Realm

Since Aurora’s security is backed by [Apache Shiro](https://shiro.apache.org), you can implement a
custom [Realm](http://shiro.apache.org/realm.html) to define organization-specific security behavior.

In addition to using Shiro's standard APIs to implement a Realm you can link against Aurora to
access the type-safe Permissions Aurora uses. See the Javadoc for `org.apache.aurora.scheduler.spi`
for more information.

## Packaging a realm module
Package your custom Realm(s) with a Guice module that exposes a `Set<Realm>` multibinding.

```java
package com.example;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.apache.shiro.realm.Realm;

public class MyRealmModule extends AbstractModule {
  @Override
  public void configure() {
    Realm myRealm = new MyRealm();

    Multibinder.newSetBinder(binder(), Realm.class).addBinding().toInstance(myRealm);
  }

  static class MyRealm implements Realm {
    // Realm implementation.
  }
}
```

To use your module in the scheduler, include it as a realm module based on its fully-qualified
class name:

```
-shiro_realm_modules=KERBEROS5_AUTHN,INI_AUTHNZ,com.example.MyRealmModule
```

# Known Issues

While the APIs and SPIs we ship with are stable as of 0.8.0, we are aware of several incremental
improvements. Please follow, vote, or send patches.

Relevant tickets:
* [AURORA-343](https://issues.apache.org/jira/browse/AURORA-343): HTTPS support
* [AURORA-1248](https://issues.apache.org/jira/browse/AURORA-1248): Client retries 4xx errors
* [AURORA-1279](https://issues.apache.org/jira/browse/AURORA-1279): Remove kerberos-specific build targets
* [AURORA-1293](https://issues.apache.org/jira/browse/AURORA-1291): Consider defining a JSON format in place of INI
* [AURORA-1179](https://issues.apache.org/jira/browse/AURORA-1179): Supported hashed passwords in security.ini
* [AURORA-1295](https://issues.apache.org/jira/browse/AURORA-1295): Support security for the ReadOnlyScheduler service
