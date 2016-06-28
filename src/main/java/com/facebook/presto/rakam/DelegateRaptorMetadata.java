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
import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.RaptorInsertTableHandle;
import com.facebook.presto.raptor.RaptorMetadata;
import com.facebook.presto.raptor.RaptorMetadataFactory;
import com.facebook.presto.raptor.RaptorOutputTableHandle;
import com.facebook.presto.raptor.RaptorTableHandle;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardDelta;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.json.JsonCodec;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class DelegateRaptorMetadata
        extends RaptorMetadata
{
    public static final String SHARD_TIME_COLUMN_NAME = "_shard_time";
    public static final ColumnMetadata SHARD_TIME_COLUMN = new ColumnMetadata(SHARD_TIME_COLUMN_NAME, TIMESTAMP, null, true);
    private final String connectorId;
    private final MetadataDao dao;
    private final ShardManager shardManager;

    @Inject
    public DelegateRaptorMetadata(String connectorId, IDBI dbi, ShardManager shardManager, JsonCodec<ShardInfo> shardInfoCodec, JsonCodec<ShardDelta> shardDeltaCodec)
    {
        super(connectorId, dbi, shardManager, shardInfoCodec, shardDeltaCodec);
        this.connectorId = connectorId;
        this.dao = onDemandDao(dbi, MetadataDao.class);
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorTableMetadata tableMetadata = super.getTableMetadata(session, tableHandle);

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.<ColumnMetadata>builder();

        boolean exists = false;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.getName().equals(SHARD_TIME_COLUMN_NAME)) {
                builder.add(SHARD_TIME_COLUMN);
                exists = true;
            }
            else {
                builder.add(column);
            }
        }

        if (!exists) {
            try {
                addColumn(session, tableHandle, SHARD_TIME_COLUMN);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            builder.add(SHARD_TIME_COLUMN);
        }

        return new ConnectorTableMetadata(tableMetadata.getTable(),
                builder.build(),
                tableMetadata.getProperties(),
                tableMetadata.getOwner(),
                tableMetadata.isSampled());
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        RaptorOutputTableHandle handle = checkType(super.beginCreateTable(session, tableMetadata, layout),
                RaptorOutputTableHandle.class, "");

        int columnId = Ints.checkedCast(handle.getColumnHandles().stream().mapToLong(col -> col.getColumnId()).max().getAsLong() + 1);
        ImmutableList<RaptorColumnHandle> columnHandles = ImmutableList.<RaptorColumnHandle>builder()
                .addAll(handle.getColumnHandles())
                .add(new RaptorColumnHandle(connectorId, SHARD_TIME_COLUMN_NAME, columnId, TIMESTAMP)).build();

        ImmutableList<Type> columnTypes = ImmutableList.<Type>builder()
                .addAll(handle.getColumnTypes())
                .add(TIMESTAMP).build();

        return new RaptorOutputTableHandle(
                handle.getConnectorId(), handle.getTransactionId(),
                handle.getSchemaName(), handle.getTableName(),
                columnHandles, columnTypes,
                handle.getSampleWeightColumnHandle(), handle.getSortColumnHandles(),
                handle.getSortOrders(), handle.getTemporalColumnHandle(),
                handle.getDistributionId(), handle.getBucketCount(),
                handle.getBucketColumnHandles());
    }

    public long addColumnInternal(ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        RaptorTableHandle table = checkType(tableHandle, RaptorTableHandle.class, "tableHandle");

        // Always add new columns to the end.
        // TODO: This needs to be updated when we support dropping columns.
        List<TableColumn> existingColumns = dao.listTableColumns(table.getSchemaName(), table.getTableName());
        TableColumn lastColumn = existingColumns.get(existingColumns.size() - 1);
        long columnId = lastColumn.getColumnId() + 1;
        int ordinalPosition = existingColumns.size();

        String type = column.getType().getTypeSignature().toString();
        dao.insertColumn(table.getTableId(), columnId, column.getName(), ordinalPosition, type, null, null);
        shardManager.addColumn(table.getTableId(), new ColumnInfo(columnId, column.getType()));
        return columnId;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RaptorInsertTableHandle handle = checkType(super.beginInsert(session, tableHandle), RaptorInsertTableHandle.class, "");

        if (handle.getColumnHandles().stream().anyMatch(e -> e.getColumnName().equals(SHARD_TIME_COLUMN_NAME))) {
            return handle;
        }

        long columnId;
        try {
            columnId = addColumnInternal(tableHandle, SHARD_TIME_COLUMN);
        }
        catch (Exception e) {
            Optional<TableColumn> tableColumn = dao.getTableColumns(handle.getTableId()).stream()
                    .filter(col -> col.getColumnName().equals(SHARD_TIME_COLUMN_NAME))
                    .findAny();

            if (tableColumn.isPresent()) {
                columnId = tableColumn.get().getColumnId();
            }
            else {
                throw Throwables.propagate(e);
            }
        }

        ImmutableList<RaptorColumnHandle> columnHandles = ImmutableList.<RaptorColumnHandle>builder()
                .addAll(handle.getColumnHandles())
                .add(new RaptorColumnHandle(connectorId, SHARD_TIME_COLUMN_NAME, columnId, TIMESTAMP)).build();

        ImmutableList<Type> columnTypes = ImmutableList.<Type>builder()
                .addAll(handle.getColumnTypes())
                .add(TIMESTAMP).build();

        return new RaptorInsertTableHandle(connectorId,
                handle.getTransactionId(),
                handle.getTableId(),
                columnHandles,
                columnTypes,
                handle.getExternalBatchId(),
                handle.getSortColumnHandles(),
                nCopies(handle.getSortColumnHandles().size(), ASC_NULLS_FIRST),
                handle.getBucketCount(),
                handle.getBucketColumnHandles());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        RaptorColumnHandle raptorColumnHandle = checkType(columnHandle, RaptorColumnHandle.class, "tableHandle");
        if (raptorColumnHandle.getColumnName().equals(SHARD_TIME_COLUMN_NAME)) {
            return SHARD_TIME_COLUMN;
        }

        return super.getColumnMetadata(session, tableHandle, columnHandle);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Map<String, ColumnHandle> columnHandles = super.getColumnHandles(session, tableHandle);

        if (columnHandles.entrySet().stream().anyMatch(e -> checkType(e.getValue(), RaptorColumnHandle.class, "").getColumnName().equals(SHARD_TIME_COLUMN_NAME))) {
            return columnHandles;
        }

        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.<String, ColumnHandle>builder()
                .putAll(columnHandles);

        RaptorColumnHandle shardTime = new RaptorColumnHandle(connectorId, SHARD_TIME_COLUMN_NAME, columnHandles.size(), TIMESTAMP);
        builder.put(shardTime.getColumnName(), shardTime);

        return builder.build();
    }

    public static class DelegateRaptorMetadataFactory
            extends RaptorMetadataFactory
    {
        private final String connectorId;
        private final IDBI dbi;
        private final ShardManager shardManager;
        private final JsonCodec<ShardInfo> shardInfoCodec;
        private final JsonCodec<ShardDelta> shardDeltaCodec;

        @Inject
        public DelegateRaptorMetadataFactory(RaptorConnectorId connectorId, @ForMetadata IDBI dbi, ShardManager shardManager, JsonCodec<ShardInfo> shardInfoCodec, JsonCodec<ShardDelta> shardDeltaCodec)
        {
            super(connectorId, dbi, shardManager, shardInfoCodec, shardDeltaCodec);
            this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
            this.dbi = requireNonNull(dbi, "dbi is null");
            this.shardManager = requireNonNull(shardManager, "shardManager is null");
            this.shardInfoCodec = requireNonNull(shardInfoCodec, "shardInfoCodec is null");
            this.shardDeltaCodec = requireNonNull(shardDeltaCodec, "shardDeltaCodec is null");
        }

        @Override
        public RaptorMetadata create()
        {
            return new DelegateRaptorMetadata(connectorId, dbi, shardManager, shardInfoCodec, shardDeltaCodec);
        }
    }
}
