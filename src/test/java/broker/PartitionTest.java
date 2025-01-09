
package broker;

import org.junit.jupiter.api.Test;
import producer.RecordBatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

    class PartitionTest {

        @Test
        public void ParitionConstructorTest() throws IOException {
            Partition parition = new Partition(23);
            System.out.println(parition);
        }

        @Test
        public void appendRecordBatchTest() throws IOException {
        Partition partition = new Partition(4);
        RecordBatch batch = new RecordBatch();

        // Append fake records
        batch.append(new byte[]{1, 3, 2, 4, 9, 12, 34, 123, 93});
        batch.append(new byte[]{45, 4, 85, 5, 9, 12, 34, 123, 93});
        batch.append(new byte[]{14, 6, 72, 1, 121, 31, 34, 123, 93});
        batch.append(new byte[]{90, 3, 2, 0, 102, 12, 34, 123, 93});

        partition.appendRecordBatch(batch);
        System.out.println("First batch appended.");
        }

        @Test
        public void createNewSegmentTest() throws IOException {
            LogSegment logSegment = new LogSegment(0,1);
            assertNotNull(logSegment); // Making Sure segment is not null
            System.out.println("Log segment created.");
        }
        @Test
        public void canAppendRecordToSegmentTest() throws IOException {
            Partition part = new Partition(5);
            RecordBatch batch = new RecordBatch();

            batch.append(new byte[]{1, 3, 2, 4, 9, 12, 34, 123, 93});
            batch.append(new byte[]{45, 4, 85, 5, 9, 12, 34, 123, 93});
            batch.append(new byte[]{14, 6, 72, 1, 121, 31, 34, 123, 93});
            batch.append(new byte[]{90, 3, 2, 0, 102, 12, 34, 123, 93});

            part.appendRecordBatch(batch);
            System.out.println("First batch appended successfully.");

        }


    }


