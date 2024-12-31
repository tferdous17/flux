package broker;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

// WIP: Fix failing test cases due to missing file, "./data/partition%d_%05d.log"
public class LogTest {
    @Test
    public void defaultConstructorTest() throws IOException {
        Log log = new Log();
        System.out.println(log);
    }

    @Test
    public void existingLogSegmentConstructorTest() throws IOException {
        Log log = new Log( new LogSegment(1,0));
        System.out.println(log);
    }

    @Test
    public void existingLogSegmentListConstructorTest() throws IOException {
        LogSegment premadeLogSegment1 = new LogSegment(1, 0);
        LogSegment premadeLogSegment2 = new LogSegment(1, 0);
        Log log = new Log(List.of(premadeLogSegment1, premadeLogSegment2));
        System.out.println(log);
    }

}
