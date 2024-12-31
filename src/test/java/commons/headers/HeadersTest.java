package commons.headers;

import commons.header.Header;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class HeadersTest {
    String key1 = "Kyoshi";
    String key2 = "Bob";
    Headers headers = new Headers();
    @Test
    public void keyOneTest() {
        // Setup
        headers.add(key1, "22".getBytes());
        headers.add(key1, "29".getBytes());
        headers.add(key1, "22".getBytes());

        ArrayList<Header> key1Test = (ArrayList<Header>) headers.headers(key1);
        System.out.println(key1Test);
    }

    @Test
    public void keyTwoTest() {
        // Setup
        headers.add(key2, "51".getBytes());
        headers.add(key2, "23".getBytes());
        headers.add(key2, "58".getBytes());

        ArrayList<Header> keyTwoTest = (ArrayList<Header>) headers.headers(key2);
        System.out.println(keyTwoTest);
    }
}
