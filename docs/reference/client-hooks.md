# Hooks for Aurora Client API

You can execute hook methods around Aurora API Client methods when they are called by the Aurora Command Line commands.

Explaining how hooks work is a bit tricky because of some indirection about what they apply to. Basically, a hook is code that executes when a particular Aurora Client API method runs, letting you extend the method's actions. The hook executes on the client side, specifically on the machine executing Aurora commands.

The catch is that hooks are associated with Aurora Client API methods, which users don't directly call. Instead, users call Aurora Command Line commands, which call Client API methods during their execution. Since which hooks run depend on which Client API methods get called, you will need to know which Command Line commands call which API methods. Later on, there is a table showing the various associations.

**Terminology Note**: From now on, "method(s)" refer to Client API methods, and "command(s)" refer to Command Line commands.

- [Hook Types](#hook-types)
- [Execution Order](#execution-order)
- [Hookable Methods](#hookable-methods)
- [Activating and Using Hooks](#activating-and-using-hooks)
- [.aurora Config File Settings](#aurora-config-file-settings)
- [Command Line](#command-line)
- [Hooks Protocol](#hooks-protocol)
  - [pre_ Methods](#pre_-methods)
  - [err_ Methods](#err_-methods)
  - [post_ Methods](#post_-methods)
- [Generic Hooks](#generic-hooks)
- [Hooks Process Checklist](#hooks-process-checklist)


## Hook Types

Hooks have three basic types, differing by when they run with respect to their associated method.

`pre_<method_name>`: When its associated method is called, the `pre_` hook executes first, then the called method. If the `pre_` hook fails, the method never runs. Later code that expected the method to succeed may be affected by this, and result in terminating the Aurora client.

Note that a `pre_` hook can error-trap internally so it does not
return `False`. Designers/contributors of new `pre_` hooks should
consider whether or not to error-trap them. You can error trap at the
highest level very generally and always pass the `pre_` hook by
returning `True`. For example:

    def pre_create(...):
      do_something()  # if do_something fails with an exception, the create_job is not attempted!
      return True

    # However...
    def pre_create(...):
      try:
        do_something()  # may cause exception
      except Exception:  # generic error trap will catch it
        pass  # and ignore the exception
      return True  # create_job will run in any case!

`post_<method_name>`: A `post_` hook executes after its associated method successfully finishes running. If it fails, the already executed method is unaffected. A `post_` hook's error is trapped, and any later operations are unaffected.

`err_<method_name>`: Executes only when its associated method returns a status other than OK or throws an exception. If an `err_` hook fails, the already executed method is unaffected. An `err_` hook's error is trapped, and any later operations are unaffected.

## Execution Order

A command with `pre_`, `post_`, and `err_` hooks defined and activated for its called method executes in the following order when the method successfully executes:

1. Command called
2. Command code executes
3. Method Called
4. `pre_` method hook runs
5. Method runs and successfully finishes
6. `post_` method hook runs
7. Command code executes
8. Command execution ends

The following is what happens when, for the same command and hooks, the method associated with the command suffers an error and does not successfully finish executing:

1. Command called
2. Command code executes
3. Method Called
4. `pre_` method hook runs
5. Method runs and fails
6. `err_` method hook runs
7. Command Code executes (if `err_` method does not end the command execution)
8. Command execution ends

Note that the `post_` and `err_` hooks for the same method can never both run for a single execution of that method.

## Hookable Methods

You can associate `pre_`, `post_`, and `err_` hooks with the following methods. Since you do not directly interact with the methods, but rather the Aurora Command Line commands that call them, for each method we also list the command(s) that can call the method. Note that a different method or methods may be called by a command depending on how the command's other code executes. Similarly, multiple commands can call the same method. We also list the methods' argument signatures, which are used by their associated hooks. <a name="Chart"></a>

  Aurora Client API Method | Client API Method Argument Signature | Aurora Command Line Command
  -------------------------| ------------------------------------- | ---------------------------
  ```create_job``` | ```self```, ```config``` | ```job create```, <code>runtask
  ```restart``` | ```self```, ```job_key```, ```shards```, ```update_config```, ```health_check_interval_seconds``` | ```job restart```
  ```kill_job``` | ```self```, ```job_key```, ```shards=None``` |  ```job kill```
  ```start_cronjob``` | ```self```, ```job_key``` | ```cron start```
  ```start_job_update``` | ```self```, ```config```, ```instances=None``` | ```update start```

Some specific examples:

* `pre_create_job` executes when a `create_job` method is called, and before the `create_job` method itself executes.

* `post_cancel_update` executes after a `cancel_update` method has successfully finished running.

* `err_kill_job` executes when the `kill_job` method is called, but doesn't successfully finish running.

## Activating and Using Hooks

By default, hooks are inactive. If you do not want to use hooks, you do not need to make any changes to your code. If you do want to use hooks, you will need to alter your `.aurora` config file to activate them both for the configuration as a whole as well as for individual `Job`s. And, of course, you will need to define in your config file what happens when a particular hook executes.

## .aurora Config File Settings

You can define a top-level `hooks` variable in any `.aurora` config file. `hooks` is a list of all objects that define hooks used by `Job`s defined in that config file. If you do not want to define any hooks for a configuration, `hooks` is optional.

    hooks = [Object_with_defined_hooks1, Object_with_defined_hooks2]

Be careful when assembling a config file using `include` on multiple smaller config files. If there are multiple files that assign a value to `hooks`, only the last assignment made will stick. For example, if `x.aurora` has `hooks = [a, b, c]` and `y.aurora` has `hooks = [d, e, f]` and `z.aurora` has, in this order, `include x.aurora` and `include y.aurora`, the `hooks` value will be `[d, e, f]`.

Also, for any `Job` that you want to use hooks with, its `Job` definition in the `.aurora` config file must set an `enable_hooks` flag to `True` (it defaults to `False`). By default, hooks are disabled and you must enable them for `Job`s of your choice.

To summarize, to use hooks for a particular job, you must both activate hooks for your config file as a whole, and for that job. Activating hooks only for individual jobs won't work, nor will only activating hooks for your config file as a whole. You must also specify the hooks' defining object in the `hooks` variable.

Recall that `.aurora` config files are written in Pystachio. So the following turns on hooks for production jobs at cluster1 and cluster2, but leaves them off for similar jobs with a defined user role. Of course, you also need to list the objects that define the hooks in your config file's `hooks` variable.

    jobs = [
            Job(enable_hooks = True, cluster = c, env = 'prod') for c in ('cluster1', 'cluster2')
           ]
    jobs.extend(
       Job(cluster = c, env = 'prod', role = getpass.getuser()) for c in ('cluster1', 'cluster2'))
       # Hooks disabled for these jobs

## Command Line

All Aurora Command Line commands now accept an `.aurora` config file as an optional parameter (some, of course, accept it as a required parameter). Whenever a command has a `.aurora` file parameter, any hooks specified and activated in the `.aurora` file can be used. For example:

    aurora job restart cluster1/role/env/app myapp.aurora

The command activates any hooks specified and activated in `myapp.aurora`. For the `restart` command, that is the only thing the `myapp.aurora` parameter does. So, if the command was the following, since there is no `.aurora` config file to specify any hooks, no hooks on the `restart` command can run.

    aurora job restart cluster1/role/env/app

## Hooks Protocol

Any object defined in the `.aurora` config file can define hook methods. You should define your hook methods within a class, and then use the class name as a value in the `hooks` list in your config file.

Note that you can define other methods in the class that its hook methods can call; all the logic of a hook does not have to be in its definition.

The following example defines a class containing a `pre_kill_job` hook definition that calls another method defined in the class.

    # Defines a method pre_kill_job
    class KillConfirmer(object):
      def confirm(self, msg):
        return raw_input(msg).lower() == 'yes'

      def pre_kill_job(self, job_key, shards=None):
        shards = ('shards %s' % shards) if shards is not None else 'all shards'
        return self.confirm('Are you sure you want to kill %s (%s)? (yes/no): '
                            % (job_key, shards))

### pre_ Methods

`pre_` methods have the signature:

    pre_<API method name>(self, <associated method's signature>)

`pre_` methods have the same signature as their associated method, with the addition of `self` as the first parameter. See the [chart](#Chart) above for the mapping of parameters to methods. When writing `pre_` methods, you can use the `*` and `**` syntax to designate that all unspecified parameters are passed in a list to the `*`ed variable and all named parameters with values are passed as name/value pairs to the `**`ed variable.

If this method returns False, the API command call aborts.

### err_ Methods

`err_` methods have the signature:

    err_<API method name>(self, exc, <associated method's signature>)

`err_` methods have the same signature as their associated method, with the addition of a first parameter `self` and a second parameter `exc`. `exc` is either a result with responseCode other than `ResponseCode.OK` or an `Exception`. See the [chart](#Chart) above for the mapping of parameters to methods. When writing `err`_ methods, you can use the `*` and `**` syntax to designate that all unspecified parameters are passed in a list to the `*`ed variable and all named parameters with values are passed as name/value pairs to the `**`ed variable.

`err_` method return codes are ignored.

### post_ Methods

`post_` methods have the signature:

    post_<API method name>(self, result, <associated method signature>)

`post_` method parameters are `self`, then `result`, followed by the same parameter signature as their associated method. `result` is the result of the associated method call. See the [chart](#chart) above for the mapping of parameters to methods. When writing `post_` methods, you can use the `*` and `**` syntax to designate that all unspecified arguments are passed in a list to the `*`ed parameter and all unspecified named arguments with values are passed as name/value pairs to the `**`ed parameter.

`post_` method return codes are ignored.

## Generic Hooks

There are seven Aurora API Methods which any of the three hook types can attach to. Thus, there are 21 possible hook/method combinations for a single `.aurora` config file. Say that you define `pre_` and `post_` hooks for the `restart` method. That leaves 19 undefined hook/method combinations; `err_restart` and the 3 `pre_`, `post_`, and `err_` hooks for each of the other 6 hookable methods. You can define what happens when any of these otherwise undefined 19 hooks execute via a generic hook, whose signature is:

    generic_hook(self, hook_config, event, method_name, result_or_err, args*, kw**)

where:

* `hook_config` is a named tuple of `config` (the Pystashio `config` object) and `job_key`.

* `event` is one of `pre`, `err`, or `post`, indicating which type of hook the genetic hook is standing in for. For example, assume no specific hooks were defined for the `restart` API command. If `generic_hook` is defined and activated, and `restart` is called, `generic_hook` will effectively run as `pre_restart`, `post_restart`, and `err_restart`. You can use a selection statement on this value so that `generic_hook` will act differently based on whether it is standing in for a `pre_`, `post_`, or `err_` hook.

* `method_name` is the Client API method name whose execution is causing this execution of the `generic_hook`.

* `args*`, `kw**` are the API method arguments and keyword arguments respectively.
* `result_or_err` is a tri-state parameter taking one of these three values:
  1. None for `pre_`hooks
  2. `result` for `post_` nooks
  3. `exc` for `err_` hooks

Example:

    # Overrides the standard do-nothing generic_hook by adding a log writing operation.
    from twitter.common import log
      class Logger(object):
        '''Adds to the log every time a hookable API method is called'''
        def generic_hook(self, hook_config, event, method_name, result_or_err, *args, **kw)
          log.info('%s: %s_%s of %s'
                   % (self.__class__.__name__, event, method_name, hook_config.job_key))

## Hooks Process Checklist

1. In your `.aurora` config file, add a `hooks` variable. Note that you may want to define a `.aurora` file only for hook definitions and then include this file in multiple other config files that you want to use the same hooks.

    hooks = []

2. In the `hooks` variable, list all objects that define hooks used by `Job`s defined in this config:

    hooks = [Object_hook_definer1, Object_hook_definer2]

3. For each job that uses hooks in this config file, add `enable_hooks = True` to the `Job` definition. Note that this is necessary even if you only want to use the generic hook.

4. Write your `pre_`, `post_`, and `err_` hook definitions as part of an object definition in your `.aurora` config file.

5. If desired, write your `generic_hook` definition as part of an object definition in your `.aurora` config file. Remember, the object must be listed as a member of `hooks`.

6. If your Aurora command line command does not otherwise take an `.aurora` config file argument, add the appropriate `.aurora` file as an argument in order to define and activate the configuration's hooks.
