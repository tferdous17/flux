/*
package broker;

import org.junit.jupiter.api.Test;
import producer.RecordBatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

// TODO: Fix failing test cases due to missing file, "./data/partition%d_%05d.log"

    class PartitionTest {
        private static final Path DATA_DIR = Paths.get("./data");
        private Partition partition;
        private static final int PARTITION_ID = 0;

        @BeforeAll
        static void setupDataDirectory() throws IOException {
            Files.createDirectories(DATA_DIR);
        }

        @BeforeEach
        void setUp() throws IOException {
            partition = new Partition(PARTITION_ID);
        }

        @AfterEach
        void tearDown() throws IOException {
            Files.list(DATA_DIR)
                    .filter(path -> path.toString().contains("partition" + PARTITION_ID))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }

        @Test
        void testAppendRecordBatch() throws IOException {
            RecordBatch batch = new RecordBatch();
            byte[] record1 = new byte[100];
            byte[] record2 = new byte[200];
            batch.append(record1);
            batch.append(record2);

            int startOffset = partition.appendRecordBatch(batch);

            assertEquals(0, startOffset);
            assertEquals(2, partition.getCurrentOffset());
            assertTrue(partition.getLog().getCurrentActiveLogSegment().getCurrentSizeInBytes() > 0);
        }

        @Test
        void testAppendEmptyRecordBatch() {
            assertThrows(IllegalArgumentException.class, () -> {
                RecordBatch emptyBatch = new RecordBatch();
                partition.appendRecordBatch(emptyBatch);
            });
        }

        @Test
        void testGetCurrentOffset() throws IOException {
            int initialOffset = partition.getCurrentOffset();

            RecordBatch batch = new RecordBatch();
            batch.append(new byte[100]);
            partition.appendRecordBatch(batch);

            assertTrue(partition.getCurrentOffset() > initialOffset);
        }

        @Test
        void testGetPartitionId() {
            assertEquals(PARTITION_ID, partition.getPartitionId());
        }

        @Test
        void testNewSegmentCreation() throws IOException {
            RecordBatch batch = new RecordBatch();
            byte[] largeRecord = new byte[1_000_000]; // 1MB record
            batch.append(largeRecord);

            partition.appendRecordBatch(batch);
            assertTrue(Files.list(DATA_DIR)
                    .filter(path -> path.toString().contains("partition" + PARTITION_ID))
                    .count() >= 1);
        }
    }

*/
