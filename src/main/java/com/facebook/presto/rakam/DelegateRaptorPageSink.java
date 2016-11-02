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

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.RaptorPageSink;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.util.TimestampMapper;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;

public class DelegateRaptorPageSink
        extends RaptorPageSink
{
    private final long epochMilli;
    private final int shardTimeColumnIndex;
    private final List<Long> columnIdx;

    public DelegateRaptorPageSink(DBI dbi, PageSorter pageSorter, StorageManager storageManager, JsonCodec<ShardInfo> shardInfoCodec, long transactionId, List<Long> columnIds, List<Type> columnTypes, List<Long> sortColumnIds, List<SortOrder> sortOrders, OptionalInt bucketCount, List<Long> bucketColumnIds, DataSize maxBufferSize, Optional<RaptorColumnHandle> temporalColumnHandle, int shardTimeColumnIndex)
    {
        super(pageSorter, storageManager, shardInfoCodec, transactionId, columnIds, columnTypes, sortColumnIds, sortOrders, bucketCount, bucketColumnIds, temporalColumnHandle, maxBufferSize);
        try (Handle handle = dbi.open()) {
            epochMilli = handle.createQuery("SELECT CURRENT_TIMESTAMP").map(TimestampMapper.FIRST).first().toInstant().toEpochMilli();
        }
        this.shardTimeColumnIndex = shardTimeColumnIndex;
        this.columnIdx = columnIds;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        Block block = RunLengthEncodedBlock.create(TIMESTAMP, epochMilli, page.getPositionCount());

        Block[] blocks;

        if (columnIdx.size() - 1 == page.getChannelCount()) {
            blocks = new Block[columnIdx.size()];
            System.arraycopy(page.getBlocks(), 0, blocks, 0, shardTimeColumnIndex);
            System.arraycopy(page.getBlocks(), shardTimeColumnIndex, blocks, shardTimeColumnIndex + 1, page.getChannelCount() - shardTimeColumnIndex);
        }
        else {
            blocks = page.getBlocks();
        }

        blocks[shardTimeColumnIndex] = block;

        return super.appendPage(new Page(blocks));
    }
}
