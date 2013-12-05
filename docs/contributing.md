Getting your ReviewBoard Account
--------------------------------
Go to https://reviews.apache.org and create an account.

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
Once you have shipits from the right committers, merge your changes in a single squash commit
and mark the review as submitted. The typical workflow is

    git checkout master
    git pull origin master
    git merge --squash my_feature_branch
    git commit
    git show master  # Verify everything looks sane
    git push origin master
    ./rbt close <RB_ID>
    git branch -d my_feature_branch

Merging Someone Else's Review
-----------------------------
Sometimes you'll need to merge someone else's RB. The typical workflow for this is

    git checkout master
    git pull origin master
    ./rbt patch -c <RB_ID>
    git show master  # Verify everything looks sane, author is correct
    git push origin master
