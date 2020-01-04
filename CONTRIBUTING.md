## Get the Source Code

First things first, you'll need the source! The Aurora source is available from Apache git:

    git clone https://gitbox.apache.org/repos/asf/aurora

Read the Style Guides
---------------------
Aurora's codebase is primarily Java and Python and conforms to the Twitter Commons styleguides for
both languages.

- [Java Style Guide](https://github.com/twitter/commons/blob/master/src/java/com/twitter/common/styleguide.md)
- [Python Style Guide](https://github.com/twitter/commons/blob/master/src/python/twitter/common/styleguide.md)

## Find Something to Do

There are issues in [Github](https://github.com/apache/aurora/issues) with the
["good first issue" label](https://github.com/apache/aurora/issues?q=is%3Aissue+label%3A%22good+first+issue%22+is%3Aopen)
that are good starting places for new Aurora contributors; pick one of these and dive in! To assign
a task to yourself, you may chime in on the issue discussion and ask one of the maintainers
to assign the issue to you, drop us a message on our Slack channel, or
email us at dev@apache.aurora.org.

The next step is to prepare your patch and then send us a Pull Request via the Github Web UI.

## Submitting a Pull Request

Follow the instructions outlined in the
[Github Documentation](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request)

If possible, make a link to the issue this PR is solving by adding the `#` followed by the number
of the issue it is addressing.

If you're unsure about who to add as a reviewer, you can default to adding Stephan Erb (StephanErb),
Mauricio Garavaglia (mauri), or Renan DelValle (ridv).
They will take care of finding an appropriate reviewer for the patch.

## Getting Your Review Merged

If you're not an Aurora committer, one of the committers will merge your change in as described
below. Generally, the last reviewer to give the review a 'Ship It!' will be responsible.

### Merging Your Own Review (Committers)

Submit a Pull Request against the master branch and click on squash and merge the PR via
the Github Web UI.


### Merging Someone Else's Review

Sometimes you'll need to merge someone else's PR. Use Github's Web UI to do this using the
squash and merge strategy.


Note for committers: some changes are often required to the commit message:

1. Ensure the the commit message does not exceed 100 characters per line.
2. Remove the "Testing Done" section. It's generally redundant (can be seen by checking the linked
  review) or entirely irrelevant to the commit itself.

## Cleaning Up

Your patch has landed, congratulations! The last thing you'll want to do before moving on to your
next fix is to clean up. You may delete the branch that served as the basis
for the PR and if the PR addresses a specific Github Issue, this issue should be closed and be tagged
with the version on which the fix landed.
