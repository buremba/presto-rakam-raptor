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
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSerde;
import com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer;
import com.facebook.presto.type.TypeUtils;
import com.google.common.primitives.Ints;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import javax.naming.LimitExceededException;

import java.util.Arrays;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.facebook.presto.operator.scalar.CombineHashFunction.getHash;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;

public class BlockRHashSet
        implements RHashSet
{
    private static final float FILL_RATIO = 0.9f;
    private final Type type;

    private int maxFill;
    private int mask;
    private int nextGroupId;
    private int[] groupIdsByHash;
    private long[] groupAddressByHash;
    private BlockBuilder currentPageBuilder;

    public BlockRHashSet(Type type, int expectedSize)
    {
        int hashSize = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashSize);
        mask = hashSize - 1;
        groupAddressByHash = new long[hashSize];
        Arrays.fill(groupAddressByHash, -1);
        groupIdsByHash = new int[hashSize];
        this.type = type;

        // Do not obey PageBuilderStatus,
        // currently we support maximum Integer.MAX_VALUE distinct items.
        currentPageBuilder = type.createBlockBuilder(new BlockBuilderStatus(), expectedSize);
    }

    public BlockRHashSet(BlockEncodingSerde serde, TypeManager typeManager, Slice slice)
    {
        BasicSliceInput input = slice.getInput();
        nextGroupId = input.readInt();

        this.type = TypeSerde.readType(typeManager, input);

        Block blockBuilder = serde.readBlockEncoding(input).readBlock(input);
        currentPageBuilder = type.createBlockBuilder(new BlockBuilderStatus(), blockBuilder.getPositionCount());
        for (int i = 0; i < blockBuilder.getPositionCount(); i++) {
            blockBuilder.writePositionTo(i, currentPageBuilder);
            currentPageBuilder.closeEntry();
        }

        maxFill = input.readInt();
        mask = input.readInt();

        groupIdsByHash = new int[input.readInt()];
        for (int i = 0; i < groupIdsByHash.length; i++) {
            groupIdsByHash[i] = input.readInt();
        }

        groupAddressByHash = new long[input.readInt()];
        for (int i = 0; i < groupIdsByHash.length; i++) {
            groupAddressByHash[i] = input.readLong();
        }
    }

    public static Block getBlock(TypeManager typeManager, BlockEncodingSerde serde, Slice serializedSet)
    {
        BasicSliceInput input = serializedSet.getInput();
        input.readInt();

        TypeSerde.readType(typeManager, input);

        return serde.readBlockEncoding(input).readBlock(input);
    }

    @Override
    public Slice serialize(BlockEncodingSerde serde)
    {
        // TODO: find a way to represent big sets
        DynamicSliceOutput output = new DynamicSliceOutput(Ints.checkedCast(getEstimatedSize()));
        output.writeInt(nextGroupId);

        TypeSerde.writeType(output, type);

        BlockEncoding encoding = currentPageBuilder.getEncoding();
        serde.writeBlockEncoding(output, encoding);
        encoding.writeBlock(output, currentPageBuilder);

        output.writeInt(maxFill);
        output.writeInt(mask);
        output.writeInt(groupIdsByHash.length);
        for (int l : groupIdsByHash) {
            output.writeInt(l);
        }
        output.writeInt(groupAddressByHash.length);
        for (long l : groupAddressByHash) {
            output.writeLong(l);
        }
        return output.slice();
    }

    @Override
    public void putIfAbsent(int position, Block page)
    {
        int rawHash = hashPosition(position, page);
        int hashPosition = getHashPosition(rawHash, mask);

        // look for an empty slot or a slot containing this key
        int groupId = -1;
        while (groupAddressByHash[hashPosition] != -1) {
            long address = groupAddressByHash[hashPosition];
            if (positionEqualsCurrentRow(decodePosition(address), position, page)) {
                // found an existing slot for this key
                groupId = groupIdsByHash[hashPosition];

                break;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        // did we find an existing group?
        if (groupId < 0) {
            groupId = addNewGroup(hashPosition, position, page);
        }
    }

    @Override
    public long getEstimatedSize()
    {
        return currentPageBuilder.getRetainedSizeInBytes() +
                sizeOf(groupAddressByHash) +
                sizeOf(groupIdsByHash) + (4 * 3) + 8;
    }

    private int hashPosition(int position, Block blocks)
    {
        return (int) getHash(HashGenerationOptimizer.INITIAL_HASH_VALUE,
                TypeUtils.hashPosition(type, blocks, position));
    }

    private boolean positionEqualsCurrentRow(int slicePosition, int position, Block blocks)
    {
        return positionEqualsRow(slicePosition, position, blocks);
    }

    private static int getHashPosition(int rawHash, int mask)
    {
        return murmurHash3(rawHash) & mask;
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private boolean positionEqualsRow(int leftPosition, int rightPosition, Block... rightBlocks)
    {
        Block rightBlock = rightBlocks[0];
        if (!TypeUtils.positionEqualsPosition(type, currentPageBuilder, leftPosition, rightBlock, rightPosition)) {
            return false;
        }
        return true;
    }

    private int addNewGroup(int hashPosition, int position, Block page)
    {
        // add the row to the open page
        type.appendTo(page, position, currentPageBuilder);
        int pagePosition = currentPageBuilder.getPositionCount() - 1;
        long address = encodeSyntheticAddress(0, pagePosition);

        // record group id in hash
        int groupId = nextGroupId++;

        groupAddressByHash[hashPosition] = address;
        groupIdsByHash[hashPosition] = groupId;

        // create new page builder if this page is full
        if (currentPageBuilder.getPositionCount() >= Integer.MAX_VALUE) {
            throw new RuntimeException(new LimitExceededException());
        }

        // increase capacity, if necessary
        if (nextGroupId >= maxFill) {
            rehash(maxFill * 2);
        }
        return groupId;
    }

    private void rehash(int size)
    {
        int newSize = arraySize(size + 1, FILL_RATIO);

        int newMask = newSize - 1;
        long[] newKey = new long[newSize];
        Arrays.fill(newKey, -1);
        int[] newValue = new int[newSize];

        int oldIndex = 0;
        for (int groupId = 0; groupId < nextGroupId; groupId++) {
            // seek to the next used slot
            while (groupAddressByHash[oldIndex] == -1) {
                oldIndex++;
            }

            // get the address for this slot
            long address = groupAddressByHash[oldIndex];

            // find an empty slot for the address
            int pos = getHashPosition(hashPosition(address), newMask);
            while (newKey[pos] != -1) {
                pos = (pos + 1) & newMask;
            }

            // record the mapping
            newKey[pos] = address;
            newValue[pos] = groupIdsByHash[oldIndex];
            oldIndex++;
        }

        this.mask = newMask;
        this.maxFill = calculateMaxFill(newSize);
        this.groupAddressByHash = newKey;
        this.groupIdsByHash = newValue;
    }

    private int hashPosition(long sliceAddress)
    {
        return TypeUtils.hashPosition(type, currentPageBuilder, decodePosition(sliceAddress));
    }

    @Override
    public void addBlock(Block page)
    {
        // get the group id for each position
        int positionCount = page.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            // get the group for the current row
            putIfAbsent(position, page);
        }
    }

    @Override
    public boolean contains(int position, Block block)
    {
        int rawHash = hashPosition(position, block);
        int hashPosition = getHashPosition(rawHash, mask);

        // look for a slot containing this key
        while (groupAddressByHash[hashPosition] != -1) {
            long address = groupAddressByHash[hashPosition];
            if (positionEqualsRow(decodePosition(address), position, block)) {
                // found an existing slot for this key
                return true;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        return false;
    }

    @Override
    public int getDistinctCount()
    {
        return nextGroupId;
    }

    @Override
    public Block getBlock()
    {
        return currentPageBuilder;
    }
}
