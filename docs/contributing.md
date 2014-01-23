Getting your ReviewBoard Account
--------------------------------
Go to https://reviews.apache.org and create an account.

Setting up your email account (committers)
------------------------------------------
Once your Apache ID has been set up you can configure your account and add ssh keys and
setup an email forwarding address at

  http://id.apache.org

Additional instructions for setting up your new committer email can be found at

  http://www.apache.org/dev/user-email.html

The recommended setup is to configure all services (mailing lists, JIRA, ReviewBoard) to
send emails to your @apache.org email address.

Setting up your ReviewBoard Environment
---------------------------------------
Run `./rbt status`. The first time this runs it will bootstrap and you will be asked to login.
Subsequent runs will cache your login credentials.

Submitting a Patch for Review
-----------------------------
Post a review with `rbt`, fill out the fields in your browser and hit Publish.

    ./rbt post -o -g

Updating an Existing Review
---------------------------
Incorporate review feedback, make some more commits, update your existing review, fill out the
fields in your browser and hit Publish.

    ./rbt post -o -r <RB_ID>

Merging Your Own Review (Committers)
------------------------------------
Once you have shipits from the right committers, merge your changes in a single commit and mark
the review as submitted. The typical workflow is:

    git checkout master
    git pull origin master
    ./rbt patch -c <RB_ID>  # Verify the automatically-generated commit message looks sane,
                            # editing if necessary.
    git show master         # Verify everything looks sane
    git push origin master
    ./rbt close <RB_ID>

Note that even if you're developing using feature branches you will not use `git merge` - each
commit will be an atomic change accompanied by a ReviewBoard entry.

Merging Someone Else's Review
-----------------------------
Sometimes you'll need to merge someone else's RB. The typical workflow for this is

    git checkout master
    git pull origin master
    ./rbt patch -c <RB_ID>
    git show master  # Verify everything looks sane, author is correct
    git push origin master
