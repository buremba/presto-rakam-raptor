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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.raptor.RaptorInsertTableHandle;
import com.facebook.presto.raptor.RaptorTableHandle;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.security.Identity;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;

import java.lang.reflect.Field;
import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.raptor.util.Types.checkType;

class ProxyConnectorMetadata implements ConnectorMetadata
{
    private final ConnectorMetadata metadata;
    private final MetadataManager metadataManager;

    private static final Session SESSION = Session.builder(new SessionPropertyManager())
            .setCatalog("rakam_raptor")
            .setLocale(Locale.ENGLISH)
            .setIdentity(new Identity("presto", Optional.<Principal>empty()))
            .setSchema("default")
            .build();
    private final MetadataDao dao;

    public ProxyConnectorMetadata(MetadataManager metadataManager, ConnectorMetadata metadata)
    {
        this.metadata = metadata;
        this.metadataManager = metadataManager;
        try {
            Field dao = metadata.getClass().getDeclaredField("dao");
            dao.setAccessible(true);
            this.dao = (MetadataDao) dao.get(metadata);
        }
        catch (IllegalAccessException | NoSuchFieldException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metadata.listSchemaNames(session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return metadata.getTableHandle(session, tableName);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        return metadata.getTableLayouts(session, table, constraint, desiredColumns);
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return metadata.getTableLayout(session, handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return metadata.getTableMetadata(session, table);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return metadata.listTables(session, schemaNameOrNull);
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return metadata.getSampleWeightColumnHandle(session, tableHandle);
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return metadata.canCreateSampledTables(session);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return metadata.getColumnHandles(session, tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return metadata.getColumnMetadata(session, tableHandle, columnHandle);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return metadata.listTableColumns(session, prefix);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        metadata.createTable(session, tableMetadata);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        metadata.dropTable(session, tableHandle);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        metadata.renameTable(session, tableHandle, newTableName);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        metadata.addColumn(session, tableHandle, column);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        metadata.renameColumn(session, tableHandle, source, target);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return metadata.beginCreateTable(session, tableMetadata);
    }

    @Override
    public void commitCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        metadata.commitCreateTable(session, tableHandle, fragments);
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        metadata.rollbackCreateTable(session, tableHandle);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorInsertTableHandle connectorInsertTableHandle = metadata.beginInsert(session, tableHandle);

        metadataManager.beginDelete(SESSION, new TableHandle("middleware", tableHandle));

        return connectorInsertTableHandle;
    }

    @Override
    public void commitInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        metadata.commitInsert(session, insertHandle, fragments);

        RaptorInsertTableHandle tableHandle = checkType(insertHandle, RaptorInsertTableHandle.class, "tableHandle is invalid");
        SchemaTableName table = dao.getTableColumns(tableHandle.getTableId()).get(0).getTable();

        RaptorTableHandle hiveTableHandle = new RaptorTableHandle(tableHandle.getConnectorId(), table.getSchemaName(), table.getTableName(), tableHandle.getTableId(), Optional.empty());
        metadataManager.commitDelete(SESSION, new TableHandle("middleware", hiveTableHandle), fragments);
    }

    @Override
    public void rollbackInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle)
    {
        metadata.rollbackInsert(session, insertHandle);

        RaptorInsertTableHandle tableHandle = checkType(insertHandle, RaptorInsertTableHandle.class, "tableHandle is invalid");
        SchemaTableName table = dao.getTableColumns(tableHandle.getTableId()).get(0).getTable();

        RaptorTableHandle hiveTableHandle = new RaptorTableHandle(tableHandle.getConnectorId(), table.getSchemaName(), table.getTableName(), tableHandle.getTableId(), Optional.empty());
        metadataManager.rollbackDelete(SESSION, new TableHandle("middleware", hiveTableHandle));
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return metadata.getUpdateRowIdColumnHandle(session, tableHandle);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return metadata.beginDelete(session, tableHandle);
    }

    @Override
    public void commitDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        metadata.commitDelete(session, tableHandle, fragments);
    }

    @Override
    public void rollbackDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        metadata.rollbackDelete(session, tableHandle);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        metadata.createView(session, viewName, viewData, replace);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        metadata.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return metadata.listViews(session, schemaNameOrNull);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return metadata.getViews(session, prefix);
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return metadata.supportsMetadataDelete(session, tableHandle, tableLayoutHandle);
    }

    @Override
    public OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return metadata.metadataDelete(session, tableHandle, tableLayoutHandle);
    }
}
