package commons.headers;

import commons.header.Header;

import java.util.*;

public class Headers implements HeadersInterface {
    private final HashMap<String, ArrayList<Header>> hashMap = new LinkedHashMap<>();

    @Override
    public HeadersInterface add(String key, byte[] value) throws IllegalStateException {
        Header newHeader = new Header(key, value);
        // First time keys
        if (!hashMap.containsKey(key)) {
            hashMap.put(key, new ArrayList<>());
            hashMap.get(key).add(newHeader);
        } else {
            hashMap.get(key).add(newHeader);
        }
        return this;
    }

    @Override
    public HeadersInterface add(Header header) throws IllegalStateException {
        String key = header.getKey();
        // First time keys
        if (!hashMap.containsKey(key)) {
            hashMap.put(key, new ArrayList<>());
            hashMap.get(key).add(header);
        } else {
            hashMap.get(key).add(header);
        }
        return this;
    }

    @Override
    public HeadersInterface remove(String key) throws IllegalStateException {
        // Key does not exist
        if (!hashMap.containsKey(key)) {
            throw new IllegalArgumentException("Key not found.");
        }

        hashMap.get(key).clear();
        return this;
    }

    @Override
    public Iterable<Header> headers(String key) {
        // Key does not exist
        if (!hashMap.containsKey(key)) {
            throw new IllegalArgumentException("Key not found.");
        }
        return hashMap.get(key);
    }

    @Override
    public Header lastHeader(String key) {
        // Key does not exist
        if (!hashMap.containsKey(key)) {
            throw new IllegalArgumentException("Key not found.");
        }

        ArrayList<Header> headers = hashMap.get(key);
        return headers.get(headers.size() - 1);
    }

    @Override
    public Header[] toArray() {
        List<Header> allHeaders = new ArrayList<>();
        for (ArrayList<Header> headerList : hashMap.values()) {
            allHeaders.addAll(headerList);
        }

        return allHeaders.toArray(new Header[0]);
    }

    @Override
    public Iterator<Header> iterator() {
        List<Header> allHeaders = new ArrayList<>();
        for (ArrayList<Header> headerList : hashMap.values()) {
            allHeaders.addAll(headerList);
        }
        return allHeaders.iterator();
    }
}