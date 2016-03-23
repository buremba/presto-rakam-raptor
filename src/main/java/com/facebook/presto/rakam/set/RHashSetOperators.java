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
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.google.common.base.Preconditions.checkArgument;

public class RHashSetOperators
{
    public static final CastSetToVarbinary CAST_SET_TO_VARBINARY = new CastSetToVarbinary();
    public static final CastVarbinaryToSet CAST_VARBINARY_TO_SET = new CastVarbinaryToSet();

    public static class CastSetToVarbinary
            extends SqlOperator
    {
        protected CastSetToVarbinary()
        {
            super(OperatorType.CAST, ImmutableList.of(typeParameter("T")), StandardTypes.VARBINARY, ImmutableList.of("set<T>"));
        }

        @Override
        public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            checkArgument(types.size() == 1, "Expected only one type");
            MethodHandle identity = MethodHandles.identity(Slice.class);
            return new ScalarFunctionImplementation(true, ImmutableList.of(true), identity, true);
        }
    }

    public static class CastVarbinaryToSet
            extends SqlOperator
    {
        protected CastVarbinaryToSet()
        {
            super(OperatorType.CAST, ImmutableList.of(typeParameter("T")), "set<T>", ImmutableList.of(StandardTypes.VARBINARY));
        }

        @Override
        public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            checkArgument(types.size() == 1, "Expected only one type");
            MethodHandle identity = MethodHandles.identity(Slice.class);
            return new ScalarFunctionImplementation(true, ImmutableList.of(true), identity, true);
        }
    }

    private RHashSetOperators()
    {
    }
}
