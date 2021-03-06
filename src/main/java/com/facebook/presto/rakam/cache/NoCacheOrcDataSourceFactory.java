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

import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.raptor.storage.ReaderAttributes;

import java.io.File;
import java.io.FileNotFoundException;

public class NoCacheOrcDataSourceFactory implements OrcDataSourceFactory
{
    @Override
    public OrcDataSource create(ReaderAttributes readerAttributes, File file)
            throws FileNotFoundException
    {
        return new FileOrcDataSource(file, readerAttributes.getMaxMergeDistance(), readerAttributes.getMaxReadSize(), readerAttributes.getStreamBufferSize(), readerAttributes.isLazyReadSmallRanges());
    }
}
