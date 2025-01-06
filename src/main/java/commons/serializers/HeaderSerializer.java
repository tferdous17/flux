package commons.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import commons.header.Header;

public class HeaderSerializer extends Serializer<Header> {
    @Override
    public void write(Kryo kryo, Output output, Header header) {
        output.writeString(header.getKey());

        byte[] value = header.getValue();
        output.writeInt(value.length);
        output.writeBytes(value);
    }

    @Override
    public Header read(Kryo kryo, Input input, Class<? extends Header> aClass) {
        String key = input.readString();

        int valueLength = input.readInt();
        byte[] value = input.readBytes(valueLength);

        return new Header(key, value);
    }
}
