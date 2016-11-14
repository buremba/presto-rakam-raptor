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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class IRaptorPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final long distributionId;
    private final Map<Integer, String> bucketToNode;
    private final List<Type> bucketTypes;

    @JsonCreator
    public IRaptorPartitioningHandle(
            @JsonProperty("distributionId") long distributionId,
            @JsonProperty("bucketToNode") Map<Integer, String> bucketToNode,
            @JsonProperty("bucketTypes") List<Type> bucketTypes)
    {
        this.distributionId = distributionId;
        this.bucketToNode = ImmutableMap.copyOf(requireNonNull(bucketToNode, "bucketToNode is null"));
        this.bucketTypes = requireNonNull(bucketTypes, "bucketTypes is null");
    }

    @JsonProperty
    public long getDistributionId()
    {
        return distributionId;
    }

    @JsonProperty
    public Map<Integer, String> getBucketToNode()
    {
        return bucketToNode;
    }

    @JsonProperty
    public List<Type> getBucketTypes()
    {
        return bucketTypes;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        IRaptorPartitioningHandle that = (IRaptorPartitioningHandle) o;
        return (distributionId == that.distributionId) &&
                Objects.equals(bucketToNode, that.bucketToNode);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(distributionId, bucketToNode);
    }

    @Override
    public String toString()
    {
        return String.valueOf(distributionId);
    }
}