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

import com.facebook.presto.rakam.DelegateRaptorMetadata.DelegateRaptorMetadataFactory;
import com.facebook.presto.raptor.RaptorConnector;
import com.facebook.presto.raptor.RaptorHandleResolver;
import com.facebook.presto.raptor.RaptorMetadataFactory;
import com.facebook.presto.raptor.RaptorModule;
import com.facebook.presto.raptor.RaptorPageSinkProvider;
import com.facebook.presto.raptor.backup.BackupModule;
import com.facebook.presto.raptor.storage.StorageModule;
import com.facebook.presto.raptor.util.CurrentNodeId;
import com.facebook.presto.raptor.util.RebindSafeMBeanServer;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.json.JsonModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Objects.requireNonNull;

public class RakamRaptorConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Module metadataModule;
    private final Map<String, Module> backupProviders;
    private final Map<String, String> optionalConfig;
    private final NodeManager nodeManager;
    private final BlockEncodingSerde blockEncodingSerde;
    private final TypeManager typeManager;
    private final PageSorter pageSorter;

    public RakamRaptorConnectorFactory(
            String name,
            Module metadataModule,
            Map<String, Module> backupProviders,
            Map<String, String> optionalConfig,
            NodeManager nodeManager,
            PageSorter pageSorter,
            BlockEncodingSerde blockEncodingSerde,
            TypeManager typeManager)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.metadataModule = requireNonNull(metadataModule, "metadataModule is null");
        this.backupProviders = ImmutableMap.copyOf(requireNonNull(backupProviders, "backupProviders is null"));
        this.optionalConfig = requireNonNull(optionalConfig, "optionalConfig is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new RaptorHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new MBeanModule(),
                    binder -> {
                        CurrentNodeId currentNodeId = new CurrentNodeId(nodeManager.getCurrentNode().getNodeIdentifier());
                        MBeanServer mbeanServer = new RebindSafeMBeanServer(getPlatformMBeanServer());

                        binder.bind(MBeanServer.class).toInstance(mbeanServer);
                        binder.bind(CurrentNodeId.class).toInstance(currentNodeId);
                        binder.bind(NodeManager.class).toInstance(nodeManager);
                        binder.bind(PageSorter.class).toInstance(pageSorter);
                        binder.bind(BlockEncodingSerde.class).toInstance(blockEncodingSerde);
                        binder.bind(TypeManager.class).toInstance(typeManager);
                    },
                    metadataModule,
                    new BackupModule(backupProviders),
                    Modules.override(new StorageModule(connectorId)).with(new AbstractConfigurationAwareModule()
                    {
                        @Override
                        protected void setup(Binder binder)
                        {
//                            binder.bind(ShardCompactionManager.class).to(DelegateShardCompactionManager.class).in(Scopes.SINGLETON);
//                            binder.bind(ProxyShardCompactor.class).in(Scopes.SINGLETON);
                        }
                    }),
                    Modules.override(new RaptorModule(connectorId)).with(new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                            binder.bind(RaptorPageSinkProvider.class).to(DelegateRaptorSinkProvider.class).in(Scopes.SINGLETON);
                            binder.bind(RaptorMetadataFactory.class).to(DelegateRaptorMetadataFactory.class).in(Scopes.SINGLETON);
                        }
                    }));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(RaptorConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
