/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.storage.durability;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Streams;
import com.google.common.io.Files;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.common.inject.Bindings;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.PruneJobUpdateHistory;
import org.apache.aurora.gen.storage.RemoveHostMaintenanceRequest;
import org.apache.aurora.gen.storage.RemoveJob;
import org.apache.aurora.gen.storage.RemoveJobUpdates;
import org.apache.aurora.gen.storage.RemoveLock;
import org.apache.aurora.gen.storage.RemoveQuota;
import org.apache.aurora.gen.storage.RemoveTasks;
import org.apache.aurora.gen.storage.SaveCronJob;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveHostAttributes;
import org.apache.aurora.gen.storage.SaveHostMaintenanceRequest;
import org.apache.aurora.gen.storage.SaveJobInstanceUpdateEvent;
import org.apache.aurora.gen.storage.SaveJobUpdate;
import org.apache.aurora.gen.storage.SaveJobUpdateEvent;
import org.apache.aurora.gen.storage.SaveLock;
import org.apache.aurora.gen.storage.SaveQuota;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.TierManager.TierManagerImpl.TierConfig;
import org.apache.aurora.scheduler.TierModule;
import org.apache.aurora.scheduler.app.LifecycleModule;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.Storage.Volatile;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.TUnion;
import org.apache.thrift.protocol.TJSONProtocol;
import org.junit.Test;

import static com.google.common.base.Charsets.UTF_8;

import static org.apache.aurora.scheduler.storage.durability.Generator.newStruct;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DataCompatibilityTest {

  private NonVolatileStorage createStorage(Persistence persistence) {
    Injector injector = Guice.createInjector(
        new DurableStorageModule(),
        new MemStorageModule(Bindings.annotatedKeyFactory(Volatile.class)),
        new LifecycleModule(),
        new TierModule(new TierConfig(
            "string-value",
            ImmutableMap.of("string-value", new TierInfo(false, false)))),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(EventSink.class).toInstance(event -> { });
            bind(Persistence.class).toInstance(persistence);
          }
        });
    return injector.getInstance(NonVolatileStorage.class);
  }

  /**
   * Ops to serve as a reference for the replacement golden values when read compatibility changes.
   */
  private static final List<Op> READ_COMPATIBILITY_OPS = ImmutableList.of(
      Op.pruneJobUpdateHistory(newStruct(PruneJobUpdateHistory.class)),
      Op.removeHostMaintenanceRequest(newStruct(RemoveHostMaintenanceRequest.class)),
      Op.removeJob(newStruct(RemoveJob.class)),
      Op.removeJobUpdate(newStruct(RemoveJobUpdates.class)),
      Op.removeLock(newStruct(RemoveLock.class)),
      Op.removeQuota(newStruct(RemoveQuota.class)),
      Op.removeTasks(newStruct(RemoveTasks.class)),
      Op.saveCronJob(newStruct(SaveCronJob.class)),
      Op.saveFrameworkId(newStruct(SaveFrameworkId.class)),
      Op.saveHostAttributes(newStruct(SaveHostAttributes.class)),
      Op.saveHostMaintenanceRequest(newStruct(SaveHostMaintenanceRequest.class)),
      Op.saveJobUpdate(newStruct(SaveJobUpdate.class)),
      Op.saveJobInstanceUpdateEvent(newStruct(SaveJobInstanceUpdateEvent.class)),
      Op.saveJobUpdateEvent(newStruct(SaveJobUpdateEvent.class)),
      Op.saveLock(newStruct(SaveLock.class)),
      Op.saveQuota(new SaveQuota()
          .setRole("role")
          .setQuota(new ResourceAggregate()
              .setResources(ImmutableSet.of(
                  Resource.numCpus(2.0),
                  Resource.diskMb(1),
                  Resource.ramMb(1))))),
      Op.saveTasks(newStruct(SaveTasks.class)));

  @Test
  public void testReadCompatibility() {
    // Verifies that storage can recover known-good serialized records.  A failure of this test case
    // indicates that the scheduler can no longer read a record that it was expected to in the past.
    // Golden values in `goldens/read-compatible` preserve serialized records that the scheduler is
    // expected to read.  At the end of a deprecation cycle, these files may need to be updated.
    // Golden file names are prefixed with an ordering ID (e.g. 2-removeJob) to prescribe a recovery
    // order.  This is This is necessary to accommodate Ops with relations (e.g. update events
    // relate to an update).

    // Sanity check that the current read-compatibility values can be replayed.
    NonVolatileStorage storage = createStorage(new TestPersistence(READ_COMPATIBILITY_OPS));
    storage.prepare();
    storage.start(stores -> { });
    storage.stop();

    File goldensDir = getGoldensDir("read-compatible");
    List<Op> goldenOps = loadGoldenSchemas(goldensDir).entrySet().stream()
        .sorted(Ordering.natural().onResultOf(entry ->
            Integer.parseInt(entry.getKey().split("\\-")[0])))
        .map(Entry::getValue)
        .map(DataCompatibilityTest::deserialize)
        .collect(Collectors.toList());

    // Ensure all currently-known Op types are represented in the goldens.
    assertEquals(
        ImmutableSet.copyOf(Op._Fields.values()),
        goldenOps.stream()
            .map(TUnion::getSetField)
            .collect(Collectors.toSet()));

    // Introduce each op one at a time to pinpoint a specific failed op.
    IntStream.range(1, goldenOps.size())
        .forEach(i -> {
          NonVolatileStorage store = createStorage(new TestPersistence(goldenOps.subList(0, i)));
          store.prepare();
          try {
            store.start(stores -> { });
          } catch (RuntimeException e) {
            Op failedOp = goldenOps.get(i - 1);
            Op currentOp = READ_COMPATIBILITY_OPS.stream()
                .filter(op -> op.getSetField() == failedOp.getSetField())
                .findFirst()
                .get();
            StringBuilder error = new StringBuilder()
                .append("**** Storage compatibility change detected ****")
                .append("\nFailed to recover when introducing ")
                .append(failedOp.getSetField().getFieldName())
                .append("\n")
                .append(failedOp)
                .append("\nIf this is expected, you may delete the associated golden file from ")
                .append(goldensDir.getPath())
                .append(",\nor you may replace the file with the latest serialized value:")
                .append("\n")
                .append(serialize(currentOp));
            fail(error.toString());
          }
          store.stop();
        });
  }

  private static File getGoldensDir(String kind) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL url = loader.getResource(
        DataCompatibilityTest.class.getPackage().getName().replaceAll("\\.", "/")
            + "/goldens/" + kind);
    return new File(url.getPath());
  }

  private static Map<String, String> loadGoldenSchemas(File goldensDir) {
    return Stream.of(goldensDir.listFiles())
        .collect(Collectors.toMap(
            File::getName,
            goldenFile -> {
              try {
                return Files.asCharSource(goldenFile, UTF_8).read();
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }
        ));
  }

  private static Map<String, String> generateOpSchemas() {
    return Stream.of(Op._Fields.values())
        .map(field -> {
          Method factory = Stream.of(Op.class.getDeclaredMethods())
              .filter(method -> method.getName().equals(field.getFieldName()))
              .findFirst()
              .get();

          Class<?> paramType = factory.getParameterTypes()[0];
          Type genericParamType = factory.getGenericParameterTypes()[0];
          try {
            return (Op) factory.invoke(null, Generator.valueFor(paramType, genericParamType));
          } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toMap(
            op -> op.getSetField().getFieldName(),
            DataCompatibilityTest::serialize
        ));
  }

  @Test
  public void testWriteFormatUnchanged() {
    // Attempts to flag any changes in the storage format.  While thorough, this check is not
    // complete.  It attempts to capture the entire schema by synthesizing a fully-populated
    // instance of each Op type.  For TUnions, the struct generator picks an arbitrary field to set,
    // meaning that it will only see one of the multiple possible schemas for any given TUnion.
    // These generated structs effectively give a view of the struct schema, which is compared to
    // golden files in `goldens/current`.

    Map<String, String> schemasByName = generateOpSchemas();
    File goldensDir = getGoldensDir("current");
    Map<String, String> goldensByName = loadGoldenSchemas(goldensDir);

    MapDifference<String, String> difference = Maps.difference(goldensByName, schemasByName);
    if (difference.areEqual()) {
      return;
    }

    StringBuilder error = new StringBuilder();
    StringBuilder remedy = new StringBuilder();

    Set<String> removedOps = difference.entriesOnlyOnLeft().keySet();
    if (!removedOps.isEmpty()) {
      error.append("Removal of storage Op(s): ").append(removedOps)
          .append("\nOps may only be removed after a release that")
          .append("\n  * formally deprecates the Op in release notes")
          .append("\n  * performs a no-op read of the Op type")
          .append("\n  * included warning logging when the Op was read")
          .append("\n  * ensures the Op is removed from storage")
          .append("\n\nHowever, you should also consider leaving the Op indefinitely and removing")
          .append("\nall fields as a safer alternative.");

      remedy.append("deleting the files ")
          .append(removedOps.stream()
              .map(removed -> new File(goldensDir, removed).getAbsolutePath())
              .collect(Collectors.joining(", ")));
    }

    String goldenChangeInstructions = Streams.concat(
        difference.entriesOnlyOnRight().entrySet().stream(),
        difference.entriesDiffering().entrySet().stream()
            .map(entry ->
                new SimpleImmutableEntry<>(entry.getKey(), entry.getValue().rightValue())))
        .map(entry -> new StringBuilder()
            .append("\n").append(new File(goldensDir, entry.getKey()).getPath()).append(":")
            .append("\n").append(entry.getValue())
            .toString())
        .collect(Collectors.joining("\n"));

    Set<String> addedOps = difference.entriesOnlyOnRight().keySet();
    if (!addedOps.isEmpty()) {
      error.append("Addition of storage Op(s): ").append(addedOps)
          .append("\nOps may only be introduced")
          .append("\n  a.) in a release that supports reading but not writing the Op")
          .append("\n  b.) in a release that writes the Op only with an operator-controlled flag");

      remedy.append("creating the following files")
          .append(goldenChangeInstructions);
    }

    Map<String, ValueDifference<String>> modified = difference.entriesDiffering();
    if (!modified.isEmpty()) {
      error.append("Schema changes to Op(s): " + modified.keySet())
          .append("\nThis check detects that changes occurred, not how the schema changed.")
          .append("\nSome guidelines for evolving schemas:")
          .append("\n  * Introducing fields: you must handle reading records that do not")
          .append("\n    yet have the field set.  This can be done with a backfill routine during")
          .append("\n    storage recovery if a field is required in some parts of the code")
          .append("\n  * Removing fields: must only be done after a release in which the field")
          .append("\n    is unused and announced as deprecated")
          .append("\n  * Changed fields: the type or thrift field ID of a field must never change");

      remedy.append("changing the following files")
          .append(goldenChangeInstructions);
    }

    fail(new StringBuilder()
        .append("**** Storage compatibility change detected ****")
        .append("\n")
        .append(error)
        .append("\n\nIf the necessary compatibility procedures have been performed,")
        .append("\nyou may clear this check by ")
        .append(remedy)
        .toString());
  }

  private static class TestPersistence implements Persistence {
    private final List<Op> ops;

    TestPersistence(List<Op> ops) {
      this.ops = ops;
    }

    @Override
    public void prepare() {
      // No-op.
    }

    @Override
    public Stream<Edit> recover() {
      return ops.stream().map(Edit::op);
    }

    @Override
    public void persist(Stream<Op> records) {
      // no-op.
    }
  }

  private static String serialize(Op op) {
    try {
      String unformattedJson =
          new String(new TSerializer(new TJSONProtocol.Factory()).serialize(op), UTF_8);

      // Pretty print the json for easier review of diffs.
      return new GsonBuilder().setPrettyPrinting().create()
          .toJson(new JsonParser().parse(unformattedJson)) + "\n";
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private static Op deserialize(String serializedOp) {
    try {
      Op op = new Op();

      String nonPrettyJson = new GsonBuilder().create()
          .toJson(new JsonParser().parse(serializedOp));

      new TDeserializer(new TJSONProtocol.Factory())
          .deserialize(op, nonPrettyJson.getBytes(UTF_8));
      return op;
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }
}
