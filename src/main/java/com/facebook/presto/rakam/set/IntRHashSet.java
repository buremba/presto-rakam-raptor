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
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.util.Types.checkType;

public class IntRHashSet
        implements RHashSet
{
    private final RoaringBitmap bitmap;

    public IntRHashSet()
    {
        bitmap = new RoaringBitmap();
    }

    public IntRHashSet(Slice slice)
    {
        RoaringBitmap bitmap = new RoaringBitmap();
        BasicSliceInput input = slice.getInput();

        input.readInt();
        try {
            bitmap.deserialize(input);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        this.bitmap = bitmap;
    }

    @Override
    public Slice serialize(BlockEncodingSerde serde)
    {
        Slice slice = Slices.allocate(bitmap.serializedSizeInBytes() + SizeOf.SIZE_OF_INT);
        SliceOutput output = slice.getOutput();
        output.writeInt(bitmap.getCardinality());

        try {
            bitmap.runOptimize();
            bitmap.serialize(output);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return slice;
    }

    @Override
    public long getEstimatedSize()
    {
        return bitmap.getSizeInBytes();
    }

    @Override
    public void putIfAbsent(int position, Block block)
    {
        bitmap.add(checkedCast(block.getLong(position, 0)));
    }

    @Override
    public boolean contains(int position, Block block)
    {
        return bitmap.contains(checkedCast(block.getLong(position, 0)));
    }

    private int checkedCast(long value)
    {
        int result = (int) value;
        if (value != result) {
            throw new IllegalArgumentException(String.format("Out of range: %d", value));
        }
        return result;
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
    public int cardinality()
    {
        return bitmap.getCardinality();
    }

    @Override
    public Block getBlock()
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), bitmap.getCardinality());
        PeekableIntIterator intIterator = bitmap.getIntIterator();
        while (intIterator.hasNext()) {
            BIGINT.writeLong(builder, intIterator.next());
        }
        return builder.build();
    }

    @Override
    public int cardinalityIntersection(TypeManager typeManager, BlockEncodingSerde serde, Slice set)
    {
        return RoaringBitmap.andCardinality(this.bitmap, new IntRHashSet(set).bitmap);
    }

    @Override
    public int cardinalitySubtract(TypeManager typeManager, BlockEncodingSerde serde, Slice set)
    {
        RoaringBitmap clone = bitmap.clone();
        subtract(clone, set);
        return clone.getCardinality();
    }

    @Override
    public void subtract(TypeManager typeManager, BlockEncodingSerde serde, Slice otherSet)
    {
        subtract(this.bitmap, otherSet);
    }

    public static void subtract(RoaringBitmap bitmap, Slice otherSet)
    {
        bitmap.andNot(new IntRHashSet(otherSet).bitmap);
    }

    @Override
    public void intersection(TypeManager typeManager, BlockEncodingSerde serde, Slice otherSet)
    {
        this.bitmap.and(new IntRHashSet(otherSet).bitmap);
    }

    @Override
    public void merge(TypeManager typeManager, BlockEncodingSerde serde, RHashSet otherSet)
    {
        this.bitmap.or(checkType(otherSet, IntRHashSet.class, "").bitmap);
    }
}
