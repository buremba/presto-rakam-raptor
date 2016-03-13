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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;

public class BigintRHashSet
        implements RHashSet
{
    private static final float FILL_RATIO = 0.9f;
    private final LongOpenHashSet set;
    private boolean nullExists = false;

    public BigintRHashSet(int expectedSize)
    {
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");
        set = new LongOpenHashSet(expectedSize, FILL_RATIO);
    }

    public BigintRHashSet(Slice slice)
    {
        BasicSliceInput input = slice.getInput();
        input.readInt();

        nullExists = input.readBoolean();

        try {
            ObjectInputStream ois = new ObjectInputStream(input);
            set = (LongOpenHashSet) ois.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Slice serialize(BlockEncodingSerde serde)
    {
        DynamicSliceOutput output = new DynamicSliceOutput((set.size() + 3) * SizeOf.SIZE_OF_LONG);

        output.writeInt(set.size());
        output.writeBoolean(nullExists);

        try {
            ObjectOutputStream ois = new ObjectOutputStream(output);
            ois.writeObject(set);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return output.slice();
    }

    @Override
    public long getEstimatedSize()
    {
        return set.size() * (SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_BYTE);
    }

    @Override
    public int getDistinctCount()
    {
        return set.size();
    }

    @Override
    public Block getBlock()
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), set.size());
        set.iterator().forEachRemaining(it -> BIGINT.writeLong(builder, it));
        return builder.build();
    }

    private LongOpenHashSet getSet()
    {
        return set;
    }

    @Override
    public int cardinalityMerge(TypeManager typeManager, BlockEncodingSerde serde, Slice otherSlice)
    {
        LongIterator iterator = new BigintRHashSet(otherSlice).getSet().iterator();
        int cardinality = 0;
        while (iterator.hasNext()) {
            if (set.contains(iterator.nextLong())) {
                cardinality++;
            }
        }
        return cardinality;
    }

    @Override
    public void addBlock(Block block)
    {
        int positionCount = block.getPositionCount();

        // get the group id for each position
        for (int position = 0; position < positionCount; position++) {
            // get the group for the current row
            putIfAbsent(position, block);
        }
    }

    @Override
    public boolean contains(int position, Block block)
    {
        if (block.isNull(position)) {
            return nullExists;
        }

        return set.contains(BIGINT.getLong(block, position));
    }

    @Override
    public void putIfAbsent(int position, Block block)
    {
        if (block.isNull(position)) {
            if (!nullExists) {
                nullExists = true;
            }

            return;
        }

        set.add(BIGINT.getLong(block, position));
    }

    public static Block getBlock(Slice serializedSet)
    {
        return new BigintRHashSet(serializedSet).getBlock();
    }
}
