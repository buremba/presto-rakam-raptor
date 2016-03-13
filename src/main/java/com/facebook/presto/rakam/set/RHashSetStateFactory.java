/*
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
package com.facebook.presto.rakam.set;

import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.util.array.ObjectBigArray;

import static java.util.Objects.requireNonNull;

public class RHashSetStateFactory
        implements AccumulatorStateFactory<RHashSetState>
{
    @Override
    public RHashSetState createSingleState()
    {
        return new SingleRHashSetState();
    }

    @Override
    public Class<? extends RHashSetState> getSingleStateClass()
    {
        return SingleRHashSetState.class;
    }

    @Override
    public RHashSetState createGroupedState()
    {
        return new GroupedRHashSetState();
    }

    @Override
    public Class<? extends RHashSetState> getGroupedStateClass()
    {
        return GroupedRHashSetState.class;
    }

    public static class GroupedRHashSetState
            extends AbstractGroupedAccumulatorState
            implements RHashSetState
    {
        private final ObjectBigArray<RHashSet> sets = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            sets.ensureCapacity(size);
        }

        @Override
        public RHashSet getSet()
        {
            return sets.get(getGroupId());
        }

        @Override
        public void set(RHashSet value)
        {
            requireNonNull(value, "value is null");
            sets.set(getGroupId(), value);
        }

        @Override
        public void addMemoryUsage(long value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return size + sets.sizeOf();
        }
    }

    public static class SingleRHashSetState
            implements RHashSetState
    {
        private RHashSet set;

        @Override
        public RHashSet getSet()
        {
            return set;
        }

        @Override
        public void addMemoryUsage(long value)
        {
            // no-op
        }

        @Override
        public void set(RHashSet value)
        {
            set = value;
        }

        @Override
        public long getEstimatedSize()
        {
            if (set == null) {
                return 0;
            }
            return set.getEstimatedSize();
        }
    }
}
