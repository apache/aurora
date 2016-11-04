Developing the Aurora Client
============================

The client is written in Python, and uses the
[Pants](http://pantsbuild.github.io/python-readme.html) build tool.


Building and Testing
--------------------

Building and testing the client code are both done using Pants. The relevant targets to know about
are:

   * Build a client executable: `./pants binary src/main/python/apache/aurora/client:aurora`
   * Test client code: `./pants test src/test/python/apache/aurora/client/cli:cli`

If you want to build a source distribution of the client, you need to run `./build-support/release/make-python-sdists`.


Creating Custom Builds
----------------------

There are situations where you may want to plug in custom logic to the Client that may not be
applicable to the open source codebase. Rather than create a whole CLI from scratch, you can
easily create your own custom, drop-in replacement aurora.pex using the pants build tool.

First, create an AuroraCommandLine implementation as an entry-point for registering customizations:

    from apache.aurora.client.cli.client import AuroraCommandLine

    class CustomAuroraCommandLine(AuroraCommandLine):
    """Custom AuroraCommandLine for your needs"""

    @property
    def name(self):
      return "your-company-aurora"

    @classmethod
    def get_description(cls):
      return 'Your Company internal Aurora client command line'

    def __init__(self):
      super(CustomAuroraCommandLine, self).__init__()
      # Add custom plugins..
      self.register_plugin(YourCustomPlugin())

    def register_nouns(self):
      super(CustomAuroraCommandLine, self).register_nouns()
      # You can even add new commands / sub-commands!
      self.register_noun(YourStartUpdateProxy())
      self.register_noun(YourDeployWorkflowCommand())

Secondly, create a main entry point:

    def proxy_main():
      client = CustomAuroraCommandLine()
      if len(sys.argv) == 1:
        sys.argv.append("-h")
      sys.exit(client.execute(sys.argv[1:]))

Finally, you can wire everything up with a pants BUILD file in your project directory:

    python_binary(
      name='aurora',
      entry_point='your_company.aurora.client:proxy_main',
      dependencies=[
        ':client_lib'
      ]
    )

    python_library(
      name='client_lib',
      sources = [
        'client.py',
        'custom_plugin.py',
        'custom_command.py',
      ],
      dependencies = [
        # The Apache Aurora client
        # Any other dependencies for your custom code
      ],
    )

Using the same commands to build the client as above (but obviously pointing to this BUILD file
instead), you will have a drop-in replacement aurora.pex file with your customizations.

Running/Debugging
------------------

For manually testing client changes against a cluster, we use [Vagrant](https://www.vagrantup.com/).
To start a virtual cluster, you need to install Vagrant, and then run `vagrant up` for the root of
the aurora workspace. This will create a vagrant host named "devcluster", with a Mesos master, a set
of Mesos agents, and an Aurora scheduler.

If you have a change you would like to test in your local cluster, you'll rebuild the client:

    vagrant ssh -c 'aurorabuild client'

Once this completes, the `aurora` command will reflect your changes.


Running/Debugging in PyCharm
-----------------------------

It's possible to use PyCharm to run and debug both the client and client tests in an IDE. In order
to do this, first run:

    build-support/python/make-pycharm-virtualenv

This script will configure a virtualenv with all of our Python requirements. Once the script
completes it will emit instructions for configuring PyCharm:

    Your PyCharm environment is now set up.  You can open the project root
    directory with PyCharm.

    Once the project is loaded:
      - open project settings
      - click 'Project Interpreter'
      - click the cog in the upper-right corner
      - click 'Add Local'
      - select 'build-support/python/pycharm.venv/bin/python'
      - click 'OK'

### Running/Debugging Tests
After following these instructions, you should now be able to run/debug tests directly from the IDE
by right-clicking on a test (or test class) and choosing to run or debug:

[![Debug Client Test](../images/debug-client-test.png)](../images/debug-client-test.png)

If you've set a breakpoint, you can see the run will now stop and let you debug:

[![Debugging Client Test](../images/debugging-client-test.png)](../images/debugging-client-test.png)

### Running/Debugging the Client
Actually running and debugging the client is unfortunately a bit more complex. You'll need to create
a Run configuration:

* Go to Run → Edit Configurations
* Click the + icon to add a new configuration.
* Choose python and name the configuration 'client'.
* Set the script path to `/your/path/to/aurora/src/main/python/apache/aurora/client/cli/client.py`
* Set the script parameters to the command you want to run (e.g. `job status <job key>`)
* Expand the Environment section and click the ellipsis to add a new environment variable
* Click the + at the bottom to add a new variable named AURORA_CONFIG_ROOT whose value is the
  path where the your cluster configuration can be found. For example, to talk to the scheduler
  running in the vagrant image, it would be set to `/your/path/to/aurora/examples/vagrant` (this
  is the directory where our example clusters.json is found).
* You should now be able to run and debug this configuration!
