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

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.AggregationMetadata;
import com.facebook.presto.operator.aggregation.GenericAccumulatorFactoryBinder;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.util.Reflection.methodHandle;

public class MergeRSetAggregation
        extends SqlAggregationFunction
{
    private static final String NAME = "merge_sets";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(MergeRSetAggregation.class, "input", Type.class, TypeManager.class, BlockEncodingSerde.class, RHashSetState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(MergeRSetAggregation.class, "combine", RHashSetState.class, RHashSetState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(MergeRSetAggregation.class, "output", Type.class, BlockEncodingSerde.class, RHashSetState.class, BlockBuilder.class);
    private final BlockEncodingSerde serde;

    public MergeRSetAggregation(BlockEncodingSerde serde)
    {
        super(NAME, ImmutableList.of(typeParameter("T")), "set<T>", ImmutableList.of("set<T>"));
        this.serde = serde;
    }

    @Override
    public String getDescription()
    {
        return "return an set of values";
    }

    @Override
    public InternalAggregationFunction specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("T");

        DynamicClassLoader classLoader = new DynamicClassLoader(RSetAggregationFunction.class.getClassLoader());

        AccumulatorStateSerializer<?> stateSerializer = new RHashSetStateSerializer(serde, typeManager, type);
        AccumulatorStateFactory<?> stateFactory = new RHashSetStateFactory();

        List<Type> inputTypes = ImmutableList.of(type);
        Type outputType = new RHashSetType(serde, typeManager, type);
        Type intermediateType = stateSerializer.getSerializedType();
        List<AggregationMetadata.ParameterMetadata> inputParameterMetadata = createInputParameterMetadata(type);

        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(type).bindTo(typeManager).bindTo(serde);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(outputType).bindTo(serde);
        Class<? extends AccumulatorState> stateInterface = RHashSetState.class;

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type, inputTypes),
                inputParameterMetadata,
                inputFunction,
                null,
                null,
                COMBINE_FUNCTION,
                outputFunction,
                stateInterface,
                stateSerializer,
                stateFactory,
                outputType,
                false);

        GenericAccumulatorFactoryBinder factory = new AccumulatorCompiler().generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, intermediateType, outputType, true, false, factory);
    }

    private static List<AggregationMetadata.ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new AggregationMetadata.ParameterMetadata(STATE), new AggregationMetadata.ParameterMetadata(BLOCK_INPUT_CHANNEL, value), new AggregationMetadata.ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type type, TypeManager typeManager, BlockEncodingSerde serde,
            RHashSetState state, Block value, int position)
    {
        RHashSet set = state.getSet();
        long startSize;
        if (set == null) {
            set = RHashSet.create(type);
            state.set(set);
            startSize = 0;
        }
        else {
            startSize = set.getEstimatedSize();
        }

        Slice slice = value.getSlice(position, 0, value.getLength(position));
        Block items = BlockRHashSet.getBlock(typeManager, serde, slice);

        set.addBlock(items);
        state.addMemoryUsage(set.getEstimatedSize() - startSize);
    }

    public static void combine(RHashSetState state, RHashSetState otherState)
    {
        RHashSet stateBlockBuilder = state.getSet();
        RHashSet otherStateBlockBuilder = otherState.getSet();
        if (otherStateBlockBuilder == null) {
            return;
        }
        if (stateBlockBuilder == null) {
            state.set(otherStateBlockBuilder);
            return;
        }
        long startSize = stateBlockBuilder.getEstimatedSize();
        state.addMemoryUsage(stateBlockBuilder.getEstimatedSize() - startSize);
    }

    public static void output(Type outputType, BlockEncodingSerde serde, RHashSetState state, BlockBuilder out)
    {
        if (state.getSet() == null) {
            out.appendNull();
        }
        else {
            outputType.writeSlice(out, state.getSet().serialize(serde));
        }
    }
}
