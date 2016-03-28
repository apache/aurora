Committer's Guide
=================

Information for official Apache Aurora committers.

Setting up your email account
-----------------------------
Once your Apache ID has been set up you can configure your account and add ssh keys and setup an
email forwarding address at

  http://id.apache.org

Additional instructions for setting up your new committer email can be found at

  http://www.apache.org/dev/user-email.html

The recommended setup is to configure all services (mailing lists, JIRA, ReviewBoard) to send
emails to your @apache.org email address.


Creating a gpg key for releases
-------------------------------
In order to create a release candidate you will need a gpg key published to an external key server
and that key will need to be added to our KEYS file as well.

1. Create a key:

               gpg --gen-key

2. Add your gpg key to the Apache Aurora KEYS file:

               git clone https://git-wip-us.apache.org/repos/asf/aurora.git
               (gpg --list-sigs <KEY ID> && gpg --armor --export <KEY ID>) >> KEYS
               git add KEYS && git commit -m "Adding gpg key for <APACHE ID>"
               ./rbt post -o -g

3. Publish the key to an external key server:

               gpg --keyserver pgp.mit.edu --send-keys <KEY ID>

4. Update the changes to the KEYS file to the Apache Aurora svn dist locations listed below:

               https://dist.apache.org/repos/dist/dev/aurora/KEYS
               https://dist.apache.org/repos/dist/release/aurora/KEYS

5. Add your key to git config for use with the release scripts:

               git config --global user.signingkey <KEY ID>


Creating a release
------------------
The following will guide you through the steps to create a release candidate, vote, and finally an
official Apache Aurora release. Before starting your gpg key should be in the KEYS file and you
must have access to commit to the dist.a.o repositories.

1. Ensure that all issues resolved for this release candidate are tagged with the correct Fix
Version in Jira, the changelog script will use this to generate the CHANGELOG in step #2.

2. Create a release candidate. This will automatically update the CHANGELOG and commit it, create a
branch and update the current version within the trunk. To create a minor version update and publish
it run

               ./build-support/release/release-candidate -l m -p

3. Update, if necessary, the draft email created from the `release-candidate` script in step #2 and
send the [VOTE] email to the dev@ mailing list. You can verify the release signature and checksums
by running

               ./build-support/release/verify-release-candidate

4. Wait for the vote to complete. If the vote fails close the vote by replying to the initial [VOTE]
email sent in step #3 by editing the subject to [RESULT][VOTE] ... and noting the failure reason
(example [here](http://markmail.org/message/d4d6xtvj7vgwi76f)). Now address any issues and go back to
step #1 and run again, this time you will use the -r flag to increment the release candidate
version. This will automatically clean up the release candidate rc0 branch and source distribution.

               ./build-support/release/release-candidate -l m -r 1 -p

5. Once the vote has successfully passed create the release

               ./build-support/release/release

6. Update the draft email created fom the `release` script in step #5 to include the Apache ID's for
all binding votes and send the [RESULT][VOTE] email to the dev@ mailing list.

