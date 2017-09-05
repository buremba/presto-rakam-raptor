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
package com.facebook.presto.rakam.cache;

import com.facebook.presto.raptor.storage.CachingOrcStorageManager;
import com.facebook.presto.raptor.storage.StorageManager;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class CacheModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(CacheConfig.class);
        CacheConfig.CacheType provider = buildConfigObject(CacheConfig.class).getCacheType();

        if(provider == CacheConfig.CacheType.heap) {
            binder.bind(OrcDataSourceFactory.class).to(HeapOrcDataSourceFactory.class);
        } else
        if(provider == CacheConfig.CacheType.memory_mapped) {
            binder.bind(OrcDataSourceFactory.class).to(MemoryMappedOrcDataSourceFactory.class);
        } else {
            binder.bind(OrcDataSourceFactory.class).to(NoCacheOrcDataSourceFactory.class);
        }
    }
}