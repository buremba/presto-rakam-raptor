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
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionType.SCALAR;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class RakamFunctionFactory
        implements FunctionFactory
{
    private final TypeManager typeManager;

    public RakamFunctionFactory(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public List<ParametricFunction> listFunctions()
    {
        return new FunctionListBuilder(typeManager)
                .function(new ArraySumFunction())
                .getFunctions();
    }

    public static class ArraySumFunction
            implements ParametricFunction
    {
        private static final Signature SIGNATURE = new Signature("array_sum", SCALAR, ImmutableList.of(typeParameter("E")), "E", ImmutableList.of("array<E>"), false);
        private static final Map<Class<?>, MethodHandle> METHOD_HANDLES = ImmutableMap.<Class<?>, MethodHandle>builder()
                .put(long.class, methodHandle(ArraySumFunction.class, "bigintArraySum", Block.class))
                .put(double.class, methodHandle(ArraySumFunction.class, "doubleArraySum", Block.class))
                .build();

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
            return "Sum over array values";
        }

        @Override
        public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            checkArgument(types.size() == 1, "Expected one type, got %s", types);
            Type elementType = types.get("E");

            MethodHandle methodHandle = METHOD_HANDLES.get(elementType.getJavaType());

            if (methodHandle == null) {
                throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "array_sum can only be used for  array<bigint> and array<double>.");
            }

            Signature signature = new Signature("array_sum", SCALAR, elementType.getTypeSignature(), parameterizedTypeName(StandardTypes.ARRAY, elementType.getTypeSignature()));

            return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), true, ImmutableList.of(false));
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
