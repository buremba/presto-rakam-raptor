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

import com.facebook.presto.raptor.RaptorPlugin;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class RakamRaptorPlugin
        extends RaptorPlugin
{
    private TypeManager typeManager;

    private NodeManager nodeManager;
    private PageSorter pageSorter;
    private ImmutableMap<String, String> optionalConfig;

    public RakamRaptorPlugin()
    {
        super("rakam_raptor", new RakamMetadataModule(), ImmutableMap.of("s3", new S3BackupStoreModule()));
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        checkState(nodeManager != null, "NodeManager has not been set");
        checkState(typeManager != null, "TypeManager has not been set");

        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(new RakamRaptorConnectorFactory(
                    "rakam_raptor",
                    new RakamMetadataModule(),
                    ImmutableMap.of("s3", new S3BackupStoreModule()),
                    optionalConfig,
                    nodeManager,
                    pageSorter,
                    typeManager)));
        }
        return ImmutableList.of();
    }

    @Inject
    public void setTypeManager(TypeManager typeManager)
    {
        super.setTypeManager(typeManager);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Inject
    public void setNodeManager(NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
        super.setNodeManager(nodeManager);
    }

    @Inject
    public void setPageSorter(PageSorter pageSorter)
    {
        this.pageSorter = pageSorter;
        super.setPageSorter(pageSorter);
    }

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
        super.setOptionalConfig(optionalConfig);
    }
}
