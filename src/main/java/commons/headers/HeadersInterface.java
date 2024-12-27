package commons.headers;

import commons.header.Header;

public interface HeadersInterface extends Iterable<Header> {
    HeadersInterface add(String key, byte[] value) throws IllegalStateException;
    HeadersInterface add(Header header) throws IllegalStateException;
    HeadersInterface remove(String key) throws IllegalStateException;
    Iterable<Header> headers(String key);
    Header lastHeader(String key);
    Header[] toArray();

}