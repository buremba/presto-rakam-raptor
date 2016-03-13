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
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionType.SCALAR;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class CardinalityRSetFunction
        implements ParametricFunction
{
    public static final CardinalityRSetFunction SET_CARDINALITY = new CardinalityRSetFunction();
    private static final Signature SIGNATURE = new Signature("cardinality", SCALAR, ImmutableList.of(typeParameter("K")), "bigint", ImmutableList.of("set<K>"), false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(RHashSet.class, "cardinality", Slice.class);

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
        checkArgument(arity == 1, "Cardinality expects only one argument");
        Type keyType = types.get("K");
        return new FunctionInfo(
                new Signature("cardinality", SCALAR, parseTypeSignature(StandardTypes.BIGINT), parameterizedTypeName("set", keyType.getTypeSignature())),
                "Returns the cardinality of the set",
                isHidden(),
                METHOD_HANDLE,
                isDeterministic(),
                false,
                ImmutableList.of(false));
    }
}
