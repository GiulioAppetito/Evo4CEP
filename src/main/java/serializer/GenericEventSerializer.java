package serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import events.GenericEvent;

import java.util.HashMap;
import java.util.Map;

public class GenericEventSerializer extends Serializer<GenericEvent> {

    @Override
    public void write(Kryo kryo, Output output, GenericEvent event) {
        try {
            output.writeLong(event.getTimestamp());
            kryo.writeObject(output, new HashMap<>(event.getAttributes()));
        } finally {
            output.flush();
        }
    }

    @Override
    public GenericEvent read(Kryo kryo, Input input, Class<GenericEvent> type) {
        long timestamp = input.readLong();
        HashMap attributes = kryo.readObject(input, HashMap.class);

        @SuppressWarnings("unchecked")
        Map<String, Object> typedAttributes = (Map<String, Object>) attributes;

        GenericEvent event = new GenericEvent(timestamp);
        typedAttributes.forEach((key, value) -> event.setAttribute(key, value));

        return event;
    }
}
