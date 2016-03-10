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
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.raptor.RaptorPlugin;
import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.spi.type.TypeManager;
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

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class RakamRaptorPlugin
        extends RaptorPlugin
{
    private MetadataManager metadataManager;
    private TypeManager typeManager;

    public RakamRaptorPlugin()
    {
        super("rakam_raptor", new AbstractConfigurationAwareModule()
        {
            @Override
            protected void setup(Binder binder)
            {
                configBinder(binder).bindConfig(JDBCConfig.class);
            }

            @ForMetadata
            @Provides
            public DataSource getDataSource(JDBCConfig config)
            {
//                HikariConfig poolConfig = new HikariConfig();
//                poolConfig.setDataSourceClassName(com.mysql.jdbc.jdbc2.optional.MysqlDataSource.class.getName());
//
//                poolConfig.setUsername(config.getUsername());
//                poolConfig.setPassword(config.getPassword());
//                poolConfig.addDataSourceProperty("databaseName", config.getDatabase());
//                poolConfig.addDataSourceProperty("serverName", config.getHost());
//                poolConfig.setMaximumPoolSize(100);
//
//                poolConfig.setPoolName("presto-metadata-pool");
//
//                return new HikariDataSource(poolConfig);

                MysqlConnectionPoolDataSource dataSource = new MysqlConnectionPoolDataSource();
                dataSource.setUser(config.getUsername());
                dataSource.setUser(config.getPassword());
                dataSource.setServerName(config.getHost());
                dataSource.setDatabaseName(config.getDatabase());

                return dataSource;
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

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == FunctionFactory.class) {
            return ImmutableList.of(type.cast(new RakamFunctionFactory(typeManager)));
        }
        return super.getServices(type);
    }

    @Inject
    public void setTypeManager(TypeManager typeManager)
    {
        super.setTypeManager(typeManager);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }
}
