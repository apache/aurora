# Generating test resources

## Background
The Aurora source repository and distributions contain several
[binary files](../src/test/resources/org/apache/thermos/root/checkpoints) to
qualify the backwards-compatibility of thermos with checkpoint data. Since
thermos persists state to disk, to be read by other components (the GC executor
and the thermos observer), it is important that we have tests that prevent
regressions affecting the ability to parse previously-written data.

## Generating test files
The files included represent persisted checkpoints that exercise different
features of thermos. The existing files should not be modified unless
we are accepting backwards incompatibility, such as with a major release.

It is not practical to write source code to generate these files on the fly,
as source would be vulnerable to drift (e.g. due to refactoring) in ways
that would undermine the goal of ensuring backwards compatibility.

The most common reason to add a new checkpoint file would be to provide
coverage for new thermos features that alter the data format. This is
accomplished by writing and running a
[job configuration](configuration-reference.md) that exercises the feature, and
copying the checkpoint file from the sandbox directory, by default this is
`/var/run/thermos/checkpoints/<aurora task id>`.
