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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static java.util.Objects.requireNonNull;

public class RHashSetType
        extends AbstractVariableWidthType
{
    private final Type elementType;

    public static final String R_HASH_SET_NAME = "set";
    private final BlockEncodingSerde serde;
    private final TypeManager typeManager;

    public RHashSetType(BlockEncodingSerde serde, TypeManager typeManager, Type elementType)
    {
        super(parameterizedTypeName(R_HASH_SET_NAME, elementType.getTypeSignature()), Slice.class);
        this.elementType = requireNonNull(elementType, "elementType is null");
        this.serde = serde;
        this.typeManager = typeManager;
    }

    public Type getElementType()
    {
        return elementType;
    }

    @Override
    public boolean isComparable()
    {
        return elementType.isComparable();
    }

    @Override
    public boolean isOrderable()
    {
        return elementType.isOrderable();
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        Block leftType = leftBlock.getObject(leftPosition, Block.class);
        Block rightType = rightBlock.getObject(rightPosition, Block.class);

        if (leftType.getPositionCount() != rightType.getPositionCount()) {
            return false;
        }

        for (int i = 0; i < leftType.getPositionCount(); i++) {
            checkElementNotNull(leftType.isNull(i), "");
            checkElementNotNull(rightType.isNull(i), "");
            if (!elementType.equalTo(leftType, i, rightType, i)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        RHashSet set = RHashSet.create(getElementType(), serde, typeManager, block.getSlice(position, 0, block.getLength(position)));

        List<Object> values = new ArrayList<>(set.getDistinctCount());

        for (int i = 0; i < set.getDistinctCount(); i++) {
            values.add(elementType.getObjectValue(session, set.getBlock(), i));
        }

        return Collections.unmodifiableList(values);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writePositionTo(position, blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getLength(position));
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }

    @Override
    public Block getObject(Block block, int position)
    {
        return block.getObject(position, Block.class);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        blockBuilder.writeObject(value).closeEntry();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, 100);
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return ImmutableList.of(getElementType());
    }

    @Override
    public String getDisplayName()
    {
        return "set<" + elementType.getDisplayName() + ">";
    }
}
