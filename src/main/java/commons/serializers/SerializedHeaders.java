package commons.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import commons.header.Header;
import commons.headers.Headers;

public class SerializedHeaders extends Serializer<Headers> {

    @Override
    public void write(Kryo kryo, Output output, Headers headers) {
        Header[] headerArr = headers.toArray();
        output.writeInt(headerArr.length);

        for (Header header : headerArr) {
            kryo.writeObject(output, header);
        }
    }

    @Override
    public Headers read(Kryo kryo, Input input, Class<? extends Headers> aClass) {
        int size = input.readInt();
        Headers headers = new Headers();

        for (int i = 0; i < size; i++) {
            Header header = kryo.readObject(input, Header.class);
            headers.add(header);
        }
        return headers;
    }
}
