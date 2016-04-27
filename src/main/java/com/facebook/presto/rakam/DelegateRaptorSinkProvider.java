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
import com.facebook.presto.raptor.RaptorInsertTableHandle;
import com.facebook.presto.raptor.RaptorOutputTableHandle;
import com.facebook.presto.raptor.RaptorPageSinkProvider;
import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.raptor.util.Types.checkType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DelegateRaptorSinkProvider
        extends RaptorPageSinkProvider
{
    private final StorageManager storageManager;
    private final PageSorter pageSorter;
    private final JsonCodec<ShardInfo> shardInfoCodec;
    private final DataSize maxBufferSize;
    private final DBI dbi;

    @Inject
    public DelegateRaptorSinkProvider(@ForMetadata ConnectionFactory connectionFactory, StorageManager storageManager, PageSorter pageSorter, JsonCodec<ShardInfo> shardInfoCodec, StorageManagerConfig config)
    {
        super(storageManager, pageSorter, shardInfoCodec, config);
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.shardInfoCodec = requireNonNull(shardInfoCodec, "shardInfoCodec is null");
        this.maxBufferSize = config.getMaxBufferSize();
        this.dbi = new DBI(connectionFactory);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        RaptorOutputTableHandle handle = checkType(tableHandle, RaptorOutputTableHandle.class, "tableHandle");

        return new DelegateRaptorPageSink(
                dbi,
                pageSorter,
                storageManager,
                shardInfoCodec,
                handle.getTransactionId(),
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                handle.getSampleWeightColumnHandle().map(RaptorColumnHandle::getColumnId),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getSortOrders(),
                handle.getBucketCount(),
                toColumnIds(handle.getBucketColumnHandles()),
                maxBufferSize,
                getShardTimeColumnIndex(handle.getColumnHandles()));
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        RaptorInsertTableHandle handle = checkType(tableHandle, RaptorInsertTableHandle.class, "tableHandle");
        return new DelegateRaptorPageSink(
                dbi,
                pageSorter,
                storageManager,
                shardInfoCodec,
                handle.getTransactionId(),
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                Optional.empty(),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getSortOrders(),
                handle.getBucketCount(),
                toColumnIds(handle.getBucketColumnHandles()),
                maxBufferSize,
                getShardTimeColumnIndex(handle.getColumnHandles()));
    }

    private static List<Long> toColumnIds(List<RaptorColumnHandle> columnHandles)
    {
        return columnHandles.stream().map(RaptorColumnHandle::getColumnId).collect(toList());
    }

    private static long getShardTimeColumnIndex(List<RaptorColumnHandle> columnHandles)
    {
        return columnHandles.stream()
                .filter(i -> isShardTimeColumnHandle(i))
                .mapToLong(i -> i.getColumnId())
                .findAny()
                .getAsLong();
    }

    private static boolean isShardTimeColumnHandle(RaptorColumnHandle columnHandle)
    {
        return columnHandle.getColumnName().equals(DelegateRaptorMetadata.SHARD_TIME_COLUMN_NAME);
    }
}
