DB Migrations
=============

Changes to the DB schema should be made in the form of migrations. This ensures that all changes
are applied correctly after a DB dump from a previous version is restored.

DB migrations are managed through a system built on top of
[MyBatis Migrations](http://www.mybatis.org/migrations/). The migrations are run automatically when
a snapshot is restored, no manual interaction is required by cluster operators.

Upgrades
--------
When adding or altering tables or changing data, a new migration class should be created under the
org.apache.aurora.scheduler.storage.db.migration package. The class should implement the
[MigrationScript](https://github.com/mybatis/migrations/blob/master/src/main/java/org/apache/ibatis/migration/MigrationScript.java)
interface (see [V001_TestMigration](../../src/test/java/org/apache/aurora/scheduler/storage/db/testmigration/V001_TestMigration.java)
as an example). The upgrade and downgrade scripts are defined in this class. When restoring a
snapshot the list of migrations on the classpath is compared to the list of applied changes in the
DB. Any changes that have not yet been applied are executed and their downgrade script is stored
alongside the changelog entry in the database to faciliate downgrades in the event of a rollback.

Downgrades
----------
If, while running migrations, a rollback is detected, i.e. a change exists in the DB changelog that
does not exist on the classpath, the downgrade script associated with each affected change is
applied.

Baselines
---------
After enough time has passed (at least 1 official release), it should be safe to baseline migrations
if desired. This can be accomplished by adding the changes from migrations directly to
[schema.sql](../../src/main/resources/org/apache/aurora/scheduler/storage/db/schema.sql), removing
the corresponding migration classes and adding a migration to remove the changelog entries.