package broker;
import producer.RecordBatch;
import broker.Log;

import java.io.IOException;


public class Partition {

    private Log log;
    private int currentOffset;

    public Partition(Log log){
        this.log = log;
        this.currentOffset = log.getLogEndOffset(); // start from the end of the offset of the log
    }

    private boolean canAppendRecordToSegment(LogSegment segment, byte[] record) {
        // Check if the segment's current size plus the record size is within the segment's allowed limit
        long maxSegmentSize = 10240;
        long currentSegmentSize = segment.getCurrentSizeInBytes();

        // Check if adding the record would exceed the max segment size
        return (currentSegmentSize + record.length) <= maxSegmentSize;
    }

    public void appendRecordBatch(RecordBatch batch) throws IOException {
        LogSegment logSegment = this.log.getCurrentActiveLogSegment();

        for(byte[] record : batch.getBatch()){
        }
    }

}
