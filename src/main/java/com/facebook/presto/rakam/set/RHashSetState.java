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

import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateMetadata;

@AccumulatorStateMetadata(stateSerializerClass = RHashSetStateSerializer.class, stateFactoryClass = RHashSetStateFactory.class)
public interface RHashSetState
        extends AccumulatorState
{
    void set(RHashSet set);

    RHashSet getSet();

    void addMemoryUsage(long value);
}
