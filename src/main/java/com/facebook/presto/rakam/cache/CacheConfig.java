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

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;

public class CacheConfig
{
    private DataSize dataSize;
    private CacheType cacheType;

    public DataSize getDataSize()
    {
        return dataSize;
    }

    public CacheType getCacheType()
    {
        return cacheType;
    }

    @Config("cache.data-size")
    public CacheConfig setDataSize(DataSize dataSize)
    {
        this.dataSize = dataSize;
        return this;
    }

    @Config("cache.method")
    public CacheConfig setCacheType(CacheType cacheType)
    {
        this.cacheType = cacheType;
        return this;
    }

    public enum CacheType {
        heap, memory_mapped
    }
}
