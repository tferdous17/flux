package commons.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.Optional;

public class OptionalSerializer extends Serializer<Optional<?>>{
    @Override
    public void write(Kryo kryo, Output output, Optional<?> optional) {
        output.writeBoolean(optional.isPresent());
        optional.ifPresent(o -> kryo.writeClassAndObject(output, o));
    }

    @Override
    public Optional<?> read(Kryo kryo, Input input, Class<? extends Optional<?>> aClass) {
        boolean isPresent = input.readBoolean();

        return isPresent ?
                Optional.of(kryo.readClassAndObject(input))
                : Optional.empty();
    }
}
