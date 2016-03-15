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

import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;

public class RHashSetStateSerializer
        implements AccumulatorStateSerializer<RHashSetState>
{
    private final BlockEncodingSerde serde;
    private final TypeManager typeManager;
    private final RHashSetType type;

    public RHashSetStateSerializer(BlockEncodingSerde serde, TypeManager typeManager, Type type)
    {
        this.serde = serde;
        this.typeManager = typeManager;
        this.type = new RHashSetType(serde, typeManager, type);
    }

    @Override
    public Type getSerializedType()
    {
        return type;
    }

    @Override
    public void serialize(RHashSetState state, BlockBuilder out)
    {
        if (state.getSet() == null) {
            out.appendNull();
        }
        else {
            type.writeSlice(out, state.getSet().serialize(serde));
        }
    }

    @Override
    public void deserialize(Block block, int index, RHashSetState state)
    {
        if (!block.isNull(index)) {
            state.set(RHashSet.create(type.getElementType(), serde, typeManager, type.getSlice(block, index)));
        }
    }
}
