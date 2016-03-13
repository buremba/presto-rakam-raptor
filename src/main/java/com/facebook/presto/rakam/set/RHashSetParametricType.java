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

import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.ParametricType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class RHashSetParametricType
        implements ParametricType
{
    private final BlockEncodingSerde serde;
    private final TypeManager typeManager;

    public RHashSetParametricType(BlockEncodingSerde serde, TypeManager typeManager)
    {
        this.serde = serde;
        this.typeManager = typeManager;
    }

    @Override
    public String getName()
    {
        return RHashSetType.R_HASH_SET_NAME;
    }

    @Override
    public RHashSetType createType(List<Type> types, List<Object> literals)
    {
        checkArgument(types.size() == 1, "Expected only one type, got %s", types);
        checkArgument(literals.isEmpty(), "Unexpected literals: %s", literals);
        return new RHashSetType(serde, typeManager, types.get(0));
    }
}
