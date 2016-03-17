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

import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.H2ShardDao;
import com.facebook.presto.raptor.metadata.MySqlShardDao;
import com.facebook.presto.raptor.metadata.ShardDao;
import com.google.inject.Binder;
import com.google.inject.Provides;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.dbpool.H2EmbeddedDataSourceModule;
import org.skife.jdbi.v2.tweak.ConnectionFactory;

import javax.inject.Singleton;
import javax.sql.DataSource;

import java.lang.annotation.Annotation;

import static com.facebook.presto.raptor.metadata.DatabaseMetadataModule.bindDaoSupplier;
import static com.facebook.presto.raptor.util.ConditionalModule.installIfPropertyEquals;

public class RakamMetadataModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindDataSource("metadata", ForMetadata.class);
    }

    @ForMetadata
    @Singleton
    @Provides
    public ConnectionFactory createConnectionFactory(@ForMetadata DataSource dataSource)
    {
        return dataSource::getConnection;
    }

    private void bindDataSource(String type, Class<? extends Annotation> annotation)
    {
        String property = type + ".db.type";

        install(installIfPropertyEquals(property, "mysql", binder -> {
            binder.install(new MysqlMetadataModule());
            bindDaoSupplier(binder, ShardDao.class, MySqlShardDao.class);
        }));

        install(installIfPropertyEquals(property, "h2", binder -> {
            binder.install(new H2EmbeddedDataSourceModule(type, annotation));
            bindDaoSupplier(binder, ShardDao.class, H2ShardDao.class);
        }));
    }
}
