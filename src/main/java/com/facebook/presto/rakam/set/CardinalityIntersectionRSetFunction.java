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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionType.SCALAR;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class CardinalityIntersectionRSetFunction
        implements ParametricFunction
{
    private static final Signature SIGNATURE = new Signature("cardinality_intersection", SCALAR, ImmutableList.of(typeParameter("K")), "bigint", ImmutableList.of("set<K>", "set<K>"), false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(CardinalityIntersectionRSetFunction.class, "cardinalityIntersection", BlockEncodingSerde.class, TypeManager.class, Type.class, Slice.class, Slice.class);
    private final BlockEncodingSerde serde;

    public CardinalityIntersectionRSetFunction(BlockEncodingSerde serde)
    {
        this.serde = serde;
    }

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return null;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 2, "Cardinality expects only two argument");
        return new FunctionInfo(
                SIGNATURE,
                "Merges two sets and returns the cardinality of the final set",
                isHidden(),
                METHOD_HANDLE.bindTo(serde).bindTo(typeManager).bindTo(types.get("K")),
                isDeterministic(),
                false,
                ImmutableList.of(false, false));
    }

    public static long cardinalityIntersection(BlockEncodingSerde serde, TypeManager typeManager, Type type, Slice set1, Slice set2)
    {
        // use bigger set as base set for optimization
        boolean isSet1Bigger = RHashSet.cardinality(set1) > RHashSet.cardinality(set2);

        RHashSet set = RHashSet.create(type, serde, typeManager, isSet1Bigger ? set1 : set2);
        Block otherItems = RHashSet.getBlock(type, typeManager, serde, isSet1Bigger ? set2 : set1);
        int cardinality = 0;
        for (int i = 0; i < otherItems.getPositionCount(); i++) {
            if (set.contains(i, otherItems)) {
                cardinality++;
            }
        }
        return cardinality;
    }
}
