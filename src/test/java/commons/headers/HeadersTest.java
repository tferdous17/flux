package commons.headers;

import commons.header.Header;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class HeadersTest {
    @Test
    public void test1() {
        String key1 = "Kyoshi";
        String key2 = "Bob";
        Headers headers = new Headers();
        headers.add(key1, "22".getBytes());
        headers.add(key1, "29".getBytes());
        headers.add(key1, "22".getBytes());
        headers.add(key2, "51".getBytes());
        headers.add(key2, "23".getBytes());
        headers.add(key2, "58".getBytes());

        ArrayList<Header> kyoshiKeys = (ArrayList<Header>) headers.headers(key1);
        ArrayList<Header> bobKeys = (ArrayList<Header>) headers.headers(key2);

        System.out.println(kyoshiKeys);
        System.out.println(bobKeys);
    }
}
