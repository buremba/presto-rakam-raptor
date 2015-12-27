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
import com.facebook.presto.raptor.RaptorPlugin;
import com.facebook.presto.raptor.metadata.ForMetadata;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import javax.inject.Singleton;
import javax.sql.DataSource;

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
//                e1.setConnectTimeout(config.getMaxConnectionWaitMillis());
//                e1.setInitialTimeout(config.getMaxConnectionWaitMillis());
//                e1.setDefaultFetchSize(config.defaultFetchSize);
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
}
