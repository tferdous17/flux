package producer;

import org.junit.jupiter.api.Test;

public class RecordBatchTest {

    @Test
    public void recordBatchDefaultConstructorTest() {
        RecordBatch batch = new RecordBatch();
        batch.printBatchDetails(); // should print 10240 bytes as max batch size
    }

    @Test
    public void recordBatchParamConstructorTest() {
        RecordBatch batch = new RecordBatch(500_000);
        batch.printBatchDetails(); // should print 500,000 as max batch size
    }

    @Test
    public void addSerializedRecordToBatchTest() {
        RecordBatch batch = new RecordBatch();
        byte[] record = {10, 39, 122, 19, 93, 34, 9};
        boolean res = batch.append(record);

        if (res) {
            System.out.println("record successfully added\n");
        } else {
            System.out.println("record NOT successfully added");
        }
    }
}
