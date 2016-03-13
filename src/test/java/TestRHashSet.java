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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.rakam.set.RHashSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRHashSet
{
    @DataProvider(name = "types")
    public static Type[][] typesProvider()
    {
        return new Type[][] {{BIGINT}, {VARCHAR}};
    }

    @Test(dataProvider = "types")
    public void testUniqueness(Type type)
            throws Exception
    {
        RHashSet rHashSet = RHashSet.create(type);

        rHashSet.addBlock(createSequenceBlock(type, 0, 50000));

        assertEquals(rHashSet.getDistinctCount(), 50000);

        rHashSet.addBlock(createSequenceBlock(type, 0, 50000));

        assertEquals(rHashSet.getDistinctCount(), 50000);

        rHashSet.addBlock(createSequenceBlock(type, 0, 100000));

        assertEquals(rHashSet.getDistinctCount(), 100000);
    }

    @Test(dataProvider = "types")
    public void testSerialize(Type type)
            throws Exception
    {
        RHashSet rHashSet = RHashSet.create(type);

        rHashSet.addBlock(createSequenceBlock(type, 0, 50000));

        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        Slice serialize = rHashSet.serialize(metadata.getBlockEncodingSerde());

        RHashSet rHashSet1 = RHashSet.create(type, metadata.getBlockEncodingSerde(), metadata.getTypeManager(), serialize);

        assertEquals(rHashSet1.getDistinctCount(), 50000);
    }

    @Test(dataProvider = "types")
    public void testGetBlock(Type type)
            throws Exception
    {
        RHashSet rHashSet = RHashSet.create(type);

        Block expected = createSequenceBlock(type, 0, 5000);
        rHashSet.addBlock(expected);
        Block actual = rHashSet.getBlock();

        assertEquals(actual.getPositionCount(), expected.getPositionCount());

        Set<Object> actualSet = IntStream.range(0, actual.getPositionCount())
                .mapToObj(i -> type.getObjectValue(SESSION, actual, i))
                .collect(Collectors.toSet());
        Set<Object> expectedSet = IntStream.range(0, actual.getPositionCount())
                .mapToObj(i -> type.getObjectValue(SESSION, expected, i))
                .collect(Collectors.toSet());

        assertEquals(actualSet, expectedSet);
    }

    @Test(dataProvider = "types")
    public void testCardinalitySerialized(Type type)
            throws Exception
    {
        RHashSet rHashSet = RHashSet.create(type);

        rHashSet.addBlock(createSequenceBlock(type, 0, 50000));

        MetadataManager metadata = MetadataManager.createTestMetadataManager();

        Slice serialize = rHashSet.serialize(metadata.getBlockEncodingSerde());

        assertEquals(RHashSet.cardinality(serialize), 50000);
    }

    @Test(dataProvider = "types")
    public void testContains(Type type)
            throws Exception
    {
        RHashSet rHashSet = RHashSet.create(type);

        Block sequenceBlock = createSequenceBlock(type, 0, 50000);
        rHashSet.addBlock(sequenceBlock);

        for (int i = 0; i < 50000; i++) {
            assertTrue(rHashSet.contains(i, sequenceBlock));
        }

        Block notSequenceBlock = createSequenceBlock(type, 50000, 100000);
        for (int i = 0; i < 50000; i++) {
            assertFalse(rHashSet.contains(i, notSequenceBlock));
        }
    }

    private Block createSequenceBlock(Type type, int start, int end)
    {
        if (type == VARCHAR) {
            return createStringSequenceBlock(start, end);
        }
        else if (type == BIGINT) {
            return createLongSequenceBlock(start, end);
        }
        else {
            throw new IllegalStateException();
        }
    }

    public static Block createStringSequenceBlock(int start, int end)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (int i = start; i < end; i++) {
            VARCHAR.writeString(builder, String.valueOf(i));
        }

        return builder.build();
    }

    public static Block createLongSequenceBlock(int start, int end)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            BIGINT.writeLong(builder, i);
        }

        return builder.build();
    }
}
