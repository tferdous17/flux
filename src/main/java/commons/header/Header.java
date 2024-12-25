package commons.header;

public class Header implements HeaderInterface {
    private final String key;
    private final byte[] value;

    public Header(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }
    @Override
    public String getKey() {
        return key;
    }
    @Override
    public byte[] getValue() {
        return value;
    }
    @Override
    public String toString() {
        return "MyHeader{" +
                "key='" + key + '\'' +
                ", value=" + new String(value) +
                '}';
    }

}
