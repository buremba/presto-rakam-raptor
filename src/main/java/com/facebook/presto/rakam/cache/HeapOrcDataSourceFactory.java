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

import com.facebook.presto.orc.AbstractOrcDataSource;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.raptor.storage.ReaderAttributes;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class HeapOrcDataSourceFactory
        implements OrcDataSourceFactory
{
    private final LoadingCache<FileRegion, byte[]> cache;
    private static final Logger logger = Logger.get(HeapOrcDataSourceFactory.class);

    @Inject
    public HeapOrcDataSourceFactory(CacheConfig config)
    {
        cache = CacheBuilder.newBuilder()
                .maximumWeight(config.getDataSize().toBytes())
                .weigher(new Weigher<FileRegion, byte[]>()
                {
                    @Override
                    public int weigh(FileRegion key, byte[] value)
                    {
                        return value.length;
                    }
                })
                .build(
                        new CacheLoader<FileRegion, byte[]>()
                        {
                            public byte[] load(FileRegion key)
                                    throws IOException
                            {
                                if (!key.file.exists()) {
                                    throw new FileNotFoundException(key.file.toString());
                                }

                                try (RandomAccessFile randomAccessFile = new RandomAccessFile(key.file, "r")) {
                                    byte[] bytes = new byte[key.length];
                                    randomAccessFile.seek(key.position);
                                    randomAccessFile.read(bytes);
                                    return bytes;
                                }
                            }
                        });
    }

    @Override
    public OrcDataSource create(ReaderAttributes readerAttributes, File file)
            throws FileNotFoundException
    {
        return new HeapMemoryOrcDataSource(file, readerAttributes.getMaxMergeDistance(), readerAttributes.getMaxReadSize(), readerAttributes.getStreamBufferSize());
    }

    public class HeapMemoryOrcDataSource
            extends AbstractOrcDataSource
    {
        private RandomAccessFile input;
        private final File file;

        public HeapMemoryOrcDataSource(File path, DataSize maxMergeDistance, DataSize maxReadSize, DataSize streamBufferSize)
                throws FileNotFoundException
        {
            super(new OrcDataSourceId(path.getPath()), path.length(), maxMergeDistance, maxReadSize, streamBufferSize);
            this.file = path;
        }

        @Override
        public void close()
                throws IOException
        {
            if (input != null) {
                input.close();
            }
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            try {
                byte[] slice = cache.get(new FileRegion(file, position, bufferLength));
                System.arraycopy(slice, 0, buffer, bufferOffset, bufferLength);
            }
            catch (Exception e) {
                logger.warn(e, "Error file fetching file");

                if (input == null) {
                    this.input = new RandomAccessFile(file, "r");
                }
                input.seek(position);
                input.readFully(buffer, bufferOffset, bufferLength);
            }
        }
    }
}
