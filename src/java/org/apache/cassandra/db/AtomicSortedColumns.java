/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.base.Function;

import edu.stanford.ppl.concurrent.SnapTreeMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.concurrent.Locks;

/**
 * A thread-safe and atomic ISortedColumns implementation.
 * Operations (in particular addAll) on this implemenation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all columns have
 * been added.
 *
 * The implementation uses snaptree (https://github.com/nbronson/snaptree),
 * and in particular it's copy-on-write clone operation to achieve its
 * atomicity guarantee.
 *
 * WARNING: removing element through getSortedColumns().iterator() is *not*
 * isolated of other operations and could actually be fully ignored in the
 * face of a concurrent. Don't use it unless in a non-concurrent context.
 */
public class AtomicSortedColumns extends ColumnFamily
{
    // Reserved values for wasteTracker field. These values must not be consecutive (see avoidReservedValues)
    private static final int TRACKER_NEVER_WASTED = 0;
    private static final int TRACKER_PESSIMISTIC_LOCKING = Integer.MAX_VALUE;

    // The granularity with which we track wasted allocation/work; we round up
    private static final int ALLOCATION_GRANULARITY_BYTES = 1024;
    // The number of bytes we have to waste in excess of our acceptable realtime rate of waste (defined below)
    private static final long EXCESS_WASTE_BYTES = 10 * 1024 * 1024L;
    private static final int EXCESS_WASTE_OFFSET = (int) (EXCESS_WASTE_BYTES / ALLOCATION_GRANULARITY_BYTES);
    // Note this is a shift, because dividing a long time and then picking the low 32 bits doesn't give correct rollover behavior
    private static final int CLOCK_SHIFT = 17;
    // CLOCK_GRANULARITY = 1^9ns >> CLOCK_SHIFT == 132us == (1/7.63)ms

    private static final int APPROX_COST_OF_SNAPTREE_CLONE = 200;
    private static final int APPROX_COST_OF_SNAPTREE_NODE = 100;

    /**
     * (clock + allocation) granularity are combined to give us an acceptable (waste) allocation rate that is defined by
     * the passage of real time of ALLOCATION_GRANULARITY_BYTES/CLOCK_GRANULARITY, or in this case 7.63Kb/ms, or 7.45Mb/s
     *
     * in wasteTracker we maintain within EXCESS_WASTE_OFFSET before the current time; whenever we waste bytes
     * we increment the current value if it is within this window, and set it to the min of the window plus our waste
     * otherwise.
     */
    private volatile int wasteTracker = TRACKER_NEVER_WASTED;

    // stores the data contents so they may be swapped with isolation/atomicity
    private volatile Holder ref;

    private static final AtomicIntegerFieldUpdater<AtomicSortedColumns> wasteTrackerUpdater = AtomicIntegerFieldUpdater.newUpdater(AtomicSortedColumns.class, "wasteTracker");
    private static final AtomicReferenceFieldUpdater<AtomicSortedColumns, Holder> refUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicSortedColumns.class, Holder.class, "ref");
    private static final Column[] EMPTY_COLUMNS = new Column[0];

    public static final ColumnFamily.Factory<AtomicSortedColumns> factory = new Factory<AtomicSortedColumns>()
    {
        public AtomicSortedColumns create(CFMetaData metadata, boolean insertReversed)
        {
            return new AtomicSortedColumns(metadata);
        }
    };

    private AtomicSortedColumns(CFMetaData metadata)
    {
        this(metadata, Holder.empty(metadata.comparator));
    }

    private AtomicSortedColumns(CFMetaData metadata, Holder holder)
    {
        super(metadata);
        this.ref = holder;
    }

    public AbstractType<?> getComparator()
    {
        return (AbstractType<?>)ref.map.comparator();
    }

    public ColumnFamily.Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new AtomicSortedColumns(metadata, ref.cloneMe());
    }

    public DeletionInfo deletionInfo()
    {
        return ref.deletionInfo;
    }

    public void delete(DeletionTime delTime)
    {
        delete(new DeletionInfo(delTime));
    }

    protected void delete(RangeTombstone tombstone)
    {
        delete(new DeletionInfo(tombstone, getComparator()));
    }

    public void delete(DeletionInfo info)
    {
        if (info.isLive())
            return;

        // Keeping deletion info for max markedForDeleteAt value
        while (true)
        {
            Holder current = ref;
            DeletionInfo newDelInfo = current.deletionInfo.copy().add(info);
            if (refUpdater.compareAndSet(this, current, current.with(newDelInfo)))
                break;
        }
    }

    public void setDeletionInfo(DeletionInfo newInfo)
    {
        ref = ref.with(newInfo);
    }

    public void purgeTombstones(int gcBefore)
    {
        while (true)
        {
            Holder current = ref;
            if (!current.deletionInfo.hasPurgeableTombstones(gcBefore))
                break;

            DeletionInfo purgedInfo = current.deletionInfo.copy();
            purgedInfo.purge(gcBefore);
            if (refUpdater.compareAndSet(this, current, current.with(purgedInfo)))
                break;
        }
    }

    public void addColumn(Column column, Allocator allocator)
    {
        Holder current, modified;
        do
        {
            current = ref;
            modified = current.cloneMe();
            modified.addColumn(column, allocator, SecondaryIndexManager.nullUpdater);
        }
        while (!refUpdater.compareAndSet(this, current, modified));
    }

    public void addAll(ColumnFamily cm, Allocator allocator, Function<Column, Column> transformation)
    {
        addAllWithSizeDelta(cm, allocator, transformation, SecondaryIndexManager.nullUpdater);
    }

    /**
     * This is only called by Memtable.resolve, so only AtomicSortedColumns needs to implement it.
     *
     * @return the difference in size seen after merging the given columns
     */
    public long addAllWithSizeDelta(ColumnFamily cm, Allocator allocator, Function<Column, Column> transformation, SecondaryIndexManager.Updater indexer)
    {
        /*
         * This operation needs atomicity and isolation. To that end, we
         * add the new column to a copy of the map (a cheap O(1) snapTree
         * clone) and atomically compare and swap when everything has been
         * added. Of course, we must not forget to update the deletion times
         * too.
         *
         * In case we are adding a lot of columns, failing the final compare
         * and swap could be expensive. To mitigate, we check we haven't been
         * beaten by another thread after every column addition. If we have,
         * we bail early, avoiding unnecessary work if possible.
         *
         * Despite this mitigation, on fast machines under heavy contention for
         * a single partition, we can end up wasting huge amounts of memory, so
         * we use a memory wasted heuristic, and fall this AtomicSortedColumns
         * instance back to pessimistic locking permanently if we detect too
         * much wasted allocation.
         *
         * Note that even when using synchronization, CAS failure is still
         * possible since other threads may be in other CASing method
         * or not yet using synchronization themselves
         */

        // Eager transform of columns so we do it only once, and not under
        // the lock if we are locking
        Column[] transformedColumns = transformColumns(cm, transformation);
        DeletionInfo deletionInfo = cm.deletionInfo();

        long sizeDelta;
        boolean monitorOwned = false;
        try
        {
            if (usePessimisticLocking())
            {
                Locks.monitorEnterUnsafe(this);
                monitorOwned = true;
            }
            do
            {
                Holder current = ref;
                sizeDelta = 0;

                DeletionInfo newDelInfo = current.deletionInfo;
                if (deletionInfo.mayModify(newDelInfo))
                {
                    newDelInfo = current.deletionInfo.copy().add(deletionInfo);
                    sizeDelta += newDelInfo.dataSize() - current.deletionInfo.dataSize();
                }
                Holder modified = current.cloneWith(newDelInfo);

                int count = 0;
                boolean complete = true;
                for (Column transformedColumn : transformedColumns)
                {
                    count++;
                    sizeDelta += modified.addColumn(transformedColumn, allocator, indexer);
                    // Bail early if we know we've been beaten
                    if (ref != current)
                    {
                        complete = false;
                        break;
                    }
                }

                if (complete && refUpdater.compareAndSet(this, current, modified))
                {
                    break;
                }
                else if (!monitorOwned)
                {
                    boolean shouldLock = usePessimisticLocking();
                    if (!shouldLock)
                    {
                        long wastedBytes = APPROX_COST_OF_SNAPTREE_CLONE + (APPROX_COST_OF_SNAPTREE_NODE * count * estimatedTreeHeight(modified.size));
                        if (newDelInfo != deletionInfo)
                            wastedBytes += deletionInfo.dataSize();
                        shouldLock = updateWastedAllocationTracker(wastedBytes);
                    }
                    if (shouldLock) {
                        Locks.monitorEnterUnsafe(this);
                        monitorOwned = true;
                    }
                }
            } while (true);
        }
        finally
        {
            if (monitorOwned)
                Locks.monitorExitUnsafe(this);
        }

        indexer.updateRowLevelIndexes();
        return sizeDelta;
    }

    private static int estimatedTreeHeight(int size)
    {
        return Integer.SIZE - Integer.numberOfTrailingZeros(size);
    }

    private static Column[] transformColumns(ColumnFamily cf, Function<Column, Column> transformation) {
        int count = cf.getColumnCount();
        Column[] transformedColumns;
        if (count == 0)
        {
            transformedColumns = EMPTY_COLUMNS;
        } else
        {
            transformedColumns = new Column[count];
            count = 0;
            for (Column column : cf)
                transformedColumns[count++] = transformation.apply(column);
        }
        return transformedColumns;
    }

    boolean usePessimisticLocking()
    {
        return wasteTracker == TRACKER_PESSIMISTIC_LOCKING;
    }

    /**
     * Update the wasted allocation tracker state based on newly wasted allocation information
     *
     * @param wastedBytes the number of bytes wasted by this thread
     * @return true if the caller should now proceed with pessimistic locking because the waste limit has been reached
     */
    private boolean updateWastedAllocationTracker(long wastedBytes) {
        // Early check for huge allocation that exceeds the limit
        if (wastedBytes < EXCESS_WASTE_BYTES)
        {
            // We round up to ensure work < granularity are still accounted for
            int wastedAllocation = ((int) (wastedBytes + ALLOCATION_GRANULARITY_BYTES - 1)) / ALLOCATION_GRANULARITY_BYTES;

            int oldTrackerValue;
            while (TRACKER_PESSIMISTIC_LOCKING != (oldTrackerValue = wasteTracker))
            {
                // Note this time value has an arbitrary offset, but is a constant rate 32 bit counter (that may wrap)
                int time = (int) (System.nanoTime() >>> CLOCK_SHIFT);
                int delta = oldTrackerValue - time;
                if (oldTrackerValue == TRACKER_NEVER_WASTED || delta >= 0 || delta < -EXCESS_WASTE_OFFSET)
                    delta = -EXCESS_WASTE_OFFSET;
                delta += wastedAllocation;
                if (delta >= 0)
                    break;
                if (wasteTrackerUpdater.compareAndSet(this, oldTrackerValue, avoidReservedValues(time + delta)))
                    return false;
            }
        }
        // We have definitely reached our waste limit so set the state if it isn't already
        wasteTrackerUpdater.set(this, TRACKER_PESSIMISTIC_LOCKING);
        // And tell the caller to proceed with pessimistic locking
        return true;
    }

    private static int avoidReservedValues(int wasteTracker)
    {
        if (wasteTracker == TRACKER_NEVER_WASTED || wasteTracker == TRACKER_PESSIMISTIC_LOCKING)
            return wasteTracker + 1;
        return wasteTracker;
    }

    public boolean replace(Column oldColumn, Column newColumn)
    {
        if (!oldColumn.name().equals(newColumn.name()))
            throw new IllegalArgumentException();

        Holder current, modified;
        boolean replaced;
        do
        {
            current = ref;
            modified = current.cloneMe();
            replaced = modified.map.replace(oldColumn.name(), oldColumn, newColumn);
        }
        while (!refUpdater.compareAndSet(this, current, modified));
        return replaced;
    }

    public void clear()
    {
        Holder current, modified;
        do
        {
            current = ref;
            modified = current.clear();
        }
        while (!refUpdater.compareAndSet(this, current, modified));
    }

    public Column getColumn(ByteBuffer name)
    {
        return ref.map.get(name);
    }

    public SortedSet<ByteBuffer> getColumnNames()
    {
        return ref.map.keySet();
    }

    public Collection<Column> getSortedColumns()
    {
        return ref.map.values();
    }

    public Collection<Column> getReverseSortedColumns()
    {
        return ref.map.descendingMap().values();
    }

    public int getColumnCount()
    {
        return ref.map.size();
    }

    public Iterator<Column> iterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(ref.map, slices);
    }

    public Iterator<Column> reverseIterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(ref.map.descendingMap(), slices);
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    private static class Holder
    {
        // This is a small optimization: DeletionInfo is mutable, but we know that we will always copy it in that class,
        // so we can safely alias one DeletionInfo.live() reference and avoid some allocations.
        private static final DeletionInfo LIVE = DeletionInfo.live();

        final SnapTreeMap<ByteBuffer, Column> map;
        final DeletionInfo deletionInfo;
        // SnapTreeMap.size() is non trivial, and this object is only mutated by one thread, so we track it trivially ourselves
        int size;

        private Holder(SnapTreeMap<ByteBuffer, Column> map, DeletionInfo deletionInfo, int size)
        {
            this.map = map;
            this.deletionInfo = deletionInfo;
            this.size = size;
        }

        static Holder empty(Comparator<? super ByteBuffer> comparator)
        {
            return new Holder(new SnapTreeMap<ByteBuffer, Column>(comparator), LIVE, 0);
        }

        Holder cloneMe()
        {
            return new Holder(map.clone(), deletionInfo, size);
        }

        Holder cloneWith(DeletionInfo newDeletionInfo)
        {
            return new Holder(map.clone(), newDeletionInfo, size);
        }

        Holder with(DeletionInfo info)
        {
            return new Holder(map, info, size);
        }

        // There is no point in cloning the underlying map to clear it
        // afterwards.
        Holder clear()
        {
            return empty(map.comparator());
        }

        long addColumn(Column column, Allocator allocator, SecondaryIndexManager.Updater indexer)
        {
            ByteBuffer name = column.name();
            while (true)
            {
                Column oldColumn = map.putIfAbsent(name, column);
                if (oldColumn == null)
                {
                    indexer.insert(column);
                    size++;
                    return column.dataSize();
                }

                Column reconciledColumn = column.reconcile(oldColumn, allocator);
                if (map.replace(name, oldColumn, reconciledColumn))
                {
                    indexer.update(oldColumn, reconciledColumn);
                    return reconciledColumn.dataSize() - oldColumn.dataSize();
                }
                // We failed to replace column due to a concurrent update or a concurrent removal. Keep trying.
                // (Currently, concurrent removal should not happen (only updates), but let us support that anyway.)
            }
        }
    }
}
