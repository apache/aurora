# Apache Aurora development environment

This directory contains [Packer](https://packer.io) scripts
to build and distribute the base development environment for Aurora.

The goal of this environment is to pre-fetch dependencies and artifacts
needed for the integration test environment so that `vagrant up` is
cheap after the box has been fetched for the first time.

As dependencies (libraries, or external packages) of Aurora change, it
will be helpful to update this box and keep this cost low.

## Updating the box

1. Download [packer](https://www.packer.io/downloads.html)

2. Modify build scripts to make the changes you want
   (e.g. install packages via `apt`)

3. Fetch the latest version of our base box

        $ vagrant box update --box ubuntu/trusty64

    The box will be stored in version-specific directories under
    `~/.vagrant.d/boxes/ubuntu-VAGRANTSLASH-trusty64/`.  Find the path to the `.ovf` file for the
    latest version of the box.  In the following step, this path will be referred to as
    `$UBUNTU_OVF`.

4. Build the new box
    Using the path from the previous step, run the following command to start the build.

        $ packer build -var 'base_box_ovf=$UBUNTU_OVF' aurora.json

    This takes a while, approximately 20 minutes.  When finished, your working directory will
    contain a file named `packer_virtualbox-ovf_virtualbox.box`.

5. Verify your box locally

        $ vagrant box add --name aurora-dev-env-testing \
          packer_virtualbox-ovf_virtualbox.box

    This will make a vagrant box named `aurora-dev-env-testing` locally available to vagrant
    (i.e. not on Vagrant Cloud).  We use a different name here to avoid confusion that could
    arise from using an unreleased base box.

    Edit the [`Vagrantfile`](../../Vagrantfile), changing the line

        config.vm.box = "apache-aurora/dev-environment"

    to

        config.vm.box = "aurora-dev-env-testing"

    At this point, you can use the box as normal to run integraion tests.

6. Upload the box to Vagrant Cloud
    Our boxes are stored [here](https://atlas.hashicorp.com/apache-aurora/boxes/dev-environment).
    You must have committer access to upload a dev image box, please
    ask in dev@aurora.apache.org if you would like to contribute.
