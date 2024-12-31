package commons.header;

import org.junit.jupiter.api.Test;

public class HeaderTest {
    @Test
    public void headerTest() {
        Header header = new Header("Kyoshi", "22".getBytes());
        System.out.println(header);
    }
}
