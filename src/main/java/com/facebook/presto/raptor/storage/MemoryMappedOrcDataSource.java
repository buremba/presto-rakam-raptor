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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.orc.AbstractOrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import static com.google.common.primitives.Ints.checkedCast;

public class MemoryMappedOrcDataSource
        extends AbstractOrcDataSource
{
    private static final Logger logger = Logger.get(MemoryMappedOrcDataSource.class);
    private static final LoadingCache<FileRegion, Slice> CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.DAYS)
            .maximumSize(1000)
            .build(
                    new CacheLoader<FileRegion, Slice>()
                    {
                        public Slice load(FileRegion key)
                                throws IOException
                        {
                            if (!key.file.exists()) {
                                throw new FileNotFoundException(key.file.toString());
                            }

                            try (RandomAccessFile randomAccessFile = new RandomAccessFile(key.file, "r");
                                    FileChannel channel = randomAccessFile.getChannel()) {
                                MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, key.position, key.length);
                                return Slices.wrappedBuffer(byteBuffer);
                            }
                        }
                    });

    private RandomAccessFile input;
    private final File file;

    public MemoryMappedOrcDataSource(File path, DataSize maxMergeDistance, DataSize maxReadSize, DataSize streamBufferSize)
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
            Slice slice = CACHE.get(new FileRegion(file, position, bufferLength));
            slice.getBytes(checkedCast(position), buffer, bufferOffset, bufferLength);
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

    public static class FileRegion
    {
        public final File file;
        public final long position;
        public final int length;

        public FileRegion(File file, long position, int length)
        {
            this.file = file;
            this.position = position;
            this.length = length;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FileRegion that = (FileRegion) o;

            if (position != that.position) {
                return false;
            }
            if (length != that.length) {
                return false;
            }
            return file.equals(that.file);
        }

        @Override
        public int hashCode()
        {
            int result = file.hashCode();
            result = 31 * result + (int) (position ^ (position >>> 32));
            result = 31 * result + length;
            return result;
        }
    }
}