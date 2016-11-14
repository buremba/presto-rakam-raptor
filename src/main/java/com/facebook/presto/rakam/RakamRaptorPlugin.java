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

import com.facebook.presto.raptor.PluginInfo;
import com.facebook.presto.raptor.RaptorPlugin;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class RakamRaptorPlugin
        extends RaptorPlugin
{
    private final String name;
    private final Module metadataModule;
    private final Map<String, Module> backupProviders;

    public RakamRaptorPlugin()
    {
        this(new RakamPluginInfo());
    }

    private RakamRaptorPlugin(PluginInfo info)
    {
        this(info.getName(), info.getMetadataModule(), info.getBackupProviders());
    }

    public RakamRaptorPlugin(String name, Module metadataModule, Map<String, Module> backupProviders)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.metadataModule = requireNonNull(metadataModule, "metadataModule is null");
        this.backupProviders = ImmutableMap.copyOf(requireNonNull(backupProviders, "backupProviders is null"));
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new RakamRaptorConnectorFactory(name, metadataModule, backupProviders));
    }
}
