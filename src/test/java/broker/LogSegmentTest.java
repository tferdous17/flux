package broker;

import org.junit.jupiter.api.Test;
import producer.RecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;

// TODO: Fix failing test cases due to missing file, "./data/partition%d_%05d.log"
public class LogSegmentTest {
    @Test
    public void normalLogSegmentConstructorTest() throws IOException {
        LogSegment logSegment = new LogSegment(0,1);
        System.out.println(logSegment);
    }

    @Test
    public void overloadedLogSegmentConstructorTest() throws IOException {
        LogSegment logSegment = new LogSegment(0,1,231L);
        System.out.println(logSegment);
    }

    @Test
    public void writeBatchToSegmentTest() throws IOException {
        LogSegment segment = new LogSegment(0, 0);
        RecordBatch batch = new RecordBatch();

        try {
            // append fake data
            batch.append(new byte[]{1, 3, 2, 4, 9, 12, 34, 123, 93});
            batch.append(new byte[]{45, 4, 85, 5, 9, 12, 34, 123, 93});
            batch.append(new byte[]{14, 6, 72, 1, 121, 31, 34, 123, 93});
            batch.append(new byte[]{90, 3, 2, 0, 102, 12, 34, 123, 93});

            // should print "INFO: Batch successfully written to segment."
            segment.writeBatchToSegment(batch);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
