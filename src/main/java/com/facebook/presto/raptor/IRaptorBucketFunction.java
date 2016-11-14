
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

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.XxHash64;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Long.parseUnsignedLong;

public class IRaptorBucketFunction
        implements BucketFunction
{
    private static final long UNSIGNED_MASK = 0x7fffffffffffffffL;
    private static final long JUMP = 1L << 31;
    private static final long CONSTANT = parseUnsignedLong("2862933555777941757");

    private final HashFunction[] functions;
    private final int bucketCount;

    public IRaptorBucketFunction(int bucketCount, List<Type> types)
    {
        checkArgument(bucketCount > 0, "bucketCount must be at least one");
        checkArgument(types != null, "types is is null");

        this.bucketCount = bucketCount;
        this.functions = new HashFunction[types.size()];
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            functions[i] = getHashFunction(type);
        }
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    @Override
    public int getBucket(Page page, int position)
    {
        long hash = 0;
        int channelCount = page.getChannelCount();
        for (int i = 0; i < channelCount; i++) {
            Block block = page.getBlock(i);

            HashFunction function = functions[i];
            long value = function.hash(block, position);
            hash = (hash * 31) + value;
        }
        int value = (int) (hash & Integer.MAX_VALUE);
        return jumpConsistentHash(value, bucketCount);
    }

    public static HashFunction getHashFunction(Type type)
    {
        if (type.equals(BIGINT)) {
            return bigintHashFunction();
        }
        else if (type.equals(INTEGER)) {
            return intHashFunction();
        }
        else if (isVarcharType(type)) {
            return varcharHashFunction();
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Bucketing is supported for BIGINT, INTEGER and VARCHAR columns and not supported for type: " + type.getDisplayName());
        }
    }

    public static int jumpConsistentHash(final long key, final int buckets) {
        long k = key;
        long b = -1;
        long j = 0;

        while (j < buckets) {
            b = j;
            k = k * CONSTANT + 1L;

            j = (long) ((b + 1L) * (JUMP / toDouble((k >>> 33) + 1L)));
        }
        return (int) b;
    }

    private static double toDouble(final long n) {
        double d = n & UNSIGNED_MASK;
        if (n < 0) {
            d += 0x1.0p63;
        }
        return d;
    }

    @VisibleForTesting
    static HashFunction bigintHashFunction()
    {
        return (block, position) -> XxHash64.hash(BIGINT.getLong(block, position));
    }

    @VisibleForTesting
    static HashFunction varcharHashFunction()
    {
        return (block, position) -> XxHash64.hash(block.getSlice(position, 0, block.getLength(position)));
    }

    @VisibleForTesting
    static HashFunction intHashFunction()
    {
        return (block, position) -> XxHash64.hash(INTEGER.getLong(block, position));
    }

    interface HashFunction
    {
        long hash(Block block, int position);
    }
}