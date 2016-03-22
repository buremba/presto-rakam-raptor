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
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.Slice;

public interface RHashSet
{
    Slice serialize(BlockEncodingSerde serde);

    long getEstimatedSize();

    void putIfAbsent(int position, Block block);

    boolean contains(int position, Block block);

    void addBlock(Block page);

    int cardinality();

    Block getBlock();

    static int cardinality(Slice serializedSet)
    {
        return serializedSet.getInput().readInt();
    }

    static Block getBlock(Type type, TypeManager typeManager, BlockEncodingSerde serde, Slice serializedSet)
    {
        if (type == BigintType.BIGINT) {
            return BigintRHashSet.getBlock(serializedSet);
        }
        else {
            return BlockRHashSet.getBlock(typeManager, serde, serializedSet);
        }
    }

    static RHashSet create(Type type)
    {
        if (type == BigintType.BIGINT) {
            return new BigintRHashSet(32);
        }
        else {
            return new BlockRHashSet(type, 32);
        }
    }

    static RHashSet create(Type type, BlockEncodingSerde serde, TypeManager typeManager, Slice slice)
    {
        if (type == BigintType.BIGINT) {
            return new BigintRHashSet(slice);
        }
        else {
            return new BlockRHashSet(serde, typeManager, slice);
        }
    }

    int cardinalityIntersection(TypeManager typeManager, BlockEncodingSerde serde, Slice set);

    int cardinalitySubtract(TypeManager typeManager, BlockEncodingSerde serde, Slice set);

    void subtract(TypeManager typeManager, BlockEncodingSerde serde, Slice set);

    void intersection(TypeManager typeManager, BlockEncodingSerde serde, Slice otherSet);

    void merge(TypeManager typeManager, BlockEncodingSerde serde, RHashSet block);
}
