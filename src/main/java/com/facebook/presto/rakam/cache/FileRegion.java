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

import java.io.File;

public class FileRegion
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
