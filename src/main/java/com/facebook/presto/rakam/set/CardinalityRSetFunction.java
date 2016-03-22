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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class CardinalityRSetFunction
        extends SqlScalarFunction
{
    private static final String FUNCTION_NAME = "cardinality";
    public static final CardinalityRSetFunction SET_CARDINALITY = new CardinalityRSetFunction();
    private static final MethodHandle METHOD_HANDLE = methodHandle(CardinalityRSetFunction.class, "mapCardinality", Slice.class);

    public CardinalityRSetFunction()
    {
        super(FUNCTION_NAME, ImmutableList.of(typeParameter("K")), "bigint", ImmutableList.of("set<K>"));
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
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Cardinality expects only one argument");
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(false),
                METHOD_HANDLE,
                isDeterministic());
    }

    // We cannot use RHashSet.cardinality directly because it's in a interface so compiler will fail. (I assume that INVOKE_DYNAMIC doesn't work for interfaces)
    public static long mapCardinality(Slice slice)
    {
        return RHashSet.cardinality(slice);
    }
}
