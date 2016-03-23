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
package com.facebook.presto.rakam;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.rakam.set.CardinalityIntersectionRSetFunction;
import com.facebook.presto.rakam.set.CardinalityRSetFunction;
import com.facebook.presto.rakam.set.ComplementRHashSet;
import com.facebook.presto.rakam.set.IntersectionRSetFunction;
import com.facebook.presto.rakam.set.MergeRSetAggregation;
import com.facebook.presto.rakam.set.RHashSetOperators;
import com.facebook.presto.rakam.set.RSetAggregationFunction;
import com.facebook.presto.rakam.set.RelativeComplementRHashSet;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class RakamFunctionFactory
        implements FunctionFactory
{
    private final TypeManager typeManager;
    private final BlockEncodingSerde serde;

    public RakamFunctionFactory(TypeManager typeManager, BlockEncodingSerde serde)
    {
        this.typeManager = typeManager;
        this.serde = serde;
    }

    @Override
    public List<SqlFunction> listFunctions()
    {
        return new FunctionListBuilder(typeManager)
                .function(ArraySumFunction.ARRAY_SUM_AGGREGATION)
                .function(new RSetAggregationFunction(serde))
                .function(RHashSetOperators.CAST_SET_TO_VARBINARY)
                .function(RHashSetOperators.CAST_VARBINARY_TO_SET)
                .function(CardinalityRSetFunction.SET_CARDINALITY)
                .function(new MergeRSetAggregation(serde))
                .function(new RelativeComplementRHashSet(serde))
                .function(new ComplementRHashSet(serde))
                .function(new IntersectionRSetFunction(serde))
                .function(new CardinalityIntersectionRSetFunction(serde))
                .getFunctions();
    }

    public static class ArraySumFunction
            extends SqlScalarFunction
    {
        public static final ArraySumFunction ARRAY_SUM_AGGREGATION = new ArraySumFunction();
        private static final String NAME = "array_sum";

        private static final Map<Class<?>, MethodHandle> METHOD_HANDLES = ImmutableMap.<Class<?>, MethodHandle>builder()
                .put(long.class, methodHandle(ArraySumFunction.class, "bigintArraySum", Block.class))
                .put(double.class, methodHandle(ArraySumFunction.class, "doubleArraySum", Block.class))
                .build();

        public ArraySumFunction()
        {
            super(NAME, ImmutableList.of(typeParameter("E")), "E", ImmutableList.of("array<E>"));
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
            return "Sum over array values";
        }

        @Override
        public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            checkArgument(types.size() == 1, "Expected one type, got %s", types);
            Type elementType = types.get("E");

            MethodHandle methodHandle = METHOD_HANDLES.get(elementType.getJavaType());

            if (methodHandle == null) {
                throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "array_sum can only be used for  array<bigint> and array<double>.");
            }

            return new ScalarFunctionImplementation(true, ImmutableList.of(false), methodHandle, isDeterministic());
        }

        public static Long bigintArraySum(Block array)
        {
            long sum = 0;
            for (int i = 0; i < array.getPositionCount(); i++) {
                sum += BIGINT.getLong(array, i);
            }

            return sum;
        }

        public static Double doubleArraySum(Block array)
        {
            double sum = 0;
            for (int i = 0; i < array.getPositionCount(); i++) {
                sum += DOUBLE.getDouble(array, i);
            }

            return sum;
        }
    }
}
