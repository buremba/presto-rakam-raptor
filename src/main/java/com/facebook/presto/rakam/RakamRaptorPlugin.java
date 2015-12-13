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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.raptor.RaptorConnectorFactory;
import com.facebook.presto.raptor.RaptorPlugin;
import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class RakamRaptorPlugin extends RaptorPlugin
{
    private MetadataManager metadataManager;

    public RakamRaptorPlugin()
    {
        super("rakam_raptor", new AbstractConfigurationAwareModule() {
            @Override
            protected void setup(Binder binder)
            {
                configBinder(binder).bindConfig(JDBCConfig.class);
            }

            @ForMetadata
            @Provides
            public DataSource getDataSource(JDBCConfig config)
            {
                MysqlConnectionPoolDataSource e1 = new MysqlConnectionPoolDataSource();
                e1.setServerName(config.getHost());
                e1.setPassword(config.getPassword());
                e1.setUser(config.getUsername());
                e1.setDatabaseName(config.getDatabase());
//                e1.setConnectTimeout(this.getMaxConnectionWaitMillis());
//                e1.setInitialTimeout(this.getMaxConnectionWaitMillis());
//                e1.setDefaultFetchSize(this.defaultFetchSize);
                return e1;
            }

            @ForMetadata
            @Singleton
            @Provides
            public ConnectionFactory createConnectionFactory(@ForMetadata DataSource dataSource)
            {
                return dataSource::getConnection;
            }

        }, ImmutableMap.of("s3", new S3BackupStoreModule()));
    }

    @Inject
    public void setMetadataManager(MetadataManager metadataManager)
    {
        this.metadataManager = metadataManager;
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            RaptorConnectorFactory raptorConnectorFactory = (RaptorConnectorFactory) super.getServices(type).get(0);

            return ImmutableList.of(type.cast(new ConnectorFactory() {
                @Override
                public String getName()
                {
                    return raptorConnectorFactory.getName();
                }

                @Override
                public Connector create(String connectorId, Map<String, String> config)
                {
                    Connector connector = raptorConnectorFactory.create(connectorId, config);
                    return new ProxyConnector(metadataManager, connector);
                }
            }));
        }

        return super.getServices(type);
    }

    private static class ProxyConnector implements Connector
    {
        private final Connector connector;
        private final MetadataManager metadataManager;

        public ProxyConnector(MetadataManager metadataManager, Connector connector)
        {
            this.connector = connector;
            this.metadataManager = metadataManager;
        }

        @Override
        public ConnectorHandleResolver getHandleResolver()
        {
            return connector.getHandleResolver();
        }

        @Override
        public ConnectorMetadata getMetadata()
        {
            ConnectorMetadata metadata = connector.getMetadata();
            return new ProxyConnectorMetadata(metadataManager, metadata);
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return connector.getSplitManager();
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider()
        {
            return connector.getPageSourceProvider();
        }

        @Override
        public ConnectorRecordSetProvider getRecordSetProvider()
        {
            return connector.getRecordSetProvider();
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider()
        {
            return connector.getPageSinkProvider();
        }

        @Override
        public ConnectorRecordSinkProvider getRecordSinkProvider()
        {
            return connector.getRecordSinkProvider();
        }

        @Override
        public ConnectorIndexResolver getIndexResolver()
        {
            return connector.getIndexResolver();
        }

        @Override
        public Set<SystemTable> getSystemTables()
        {
            return connector.getSystemTables();
        }

        @Override
        public List<PropertyMetadata<?>> getSessionProperties()
        {
            return connector.getSessionProperties();
        }

        @Override
        public List<PropertyMetadata<?>> getTableProperties()
        {
            return connector.getTableProperties();
        }

        @Override
        public ConnectorAccessControl getAccessControl()
        {
            return connector.getAccessControl();
        }

        @Override
        public void shutdown()
        {
            connector.shutdown();
        }
    }
}
