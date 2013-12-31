/*
 * Copyright 2013 Twitter, Inc.
 *
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
package org.apache.aurora.scheduler.storage;

/**
 * Storage mechanism that is able to create complete snapshots of the local storage system state
 * and apply these to restore local storage from a snapshotted baseline.
 */
public interface SnapshotStore<T> {

  /**
   * Creates a consistent snapshot of the local storage system.
   *
   * @return A blob that can be used to recover local storage via {@link #applySnapshot(Object)}.
   */
   T createSnapshot();

  /**
   * Applies a snapshot blob to the local storage system, wiping out all existing data and
   * resetting with the contents of the snapshot.
   *
   * @param snapshot A snapshot blob created by {@link #createSnapshot()}.
   */
  void applySnapshot(T snapshot);
}
