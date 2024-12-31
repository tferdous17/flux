package broker;

import org.junit.jupiter.api.Test;

import java.io.IOException;

// WIP: Fix failing test cases due to missing file, "./data/partition%d_%05d.log"
public class LogSegmentTest {
    @Test
    public void normalLogSegmentConstructorTest() throws IOException {
        LogSegment logSegment = new LogSegment(0,1);
        System.out.println(logSegment);
    }

    @Test
    public void overloadedLogSegmentConstructorTest() throws  IOException {
        LogSegment logSegment = new LogSegment(0,1,231L);
        System.out.println(logSegment);
    }
}
