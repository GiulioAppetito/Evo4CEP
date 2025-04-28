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
            // Serializza il timestamp
            output.writeLong(event.getTimestamp());
            // Serializza la mappa degli attributi
            kryo.writeObject(output, new HashMap<>(event.getAttributes()));
        } finally {
            output.flush();
        }
    }

    @Override
    public GenericEvent read(Kryo kryo, Input input, Class<GenericEvent> type) {
        // Legge il timestamp
        long timestamp = input.readLong();
        // Legge la mappa degli attributi
        HashMap attributes = kryo.readObject(input, HashMap.class);

        // Cast the attributes to a type-safe map
        @SuppressWarnings("unchecked")
        Map<String, Object> typedAttributes = (Map<String, Object>) attributes;

        // Set each attribute to the event using a lambda to ensure type safety
        GenericEvent event = new GenericEvent(timestamp);
        typedAttributes.forEach((key, value) -> event.setAttribute(key, value));

        return event;
    }
}
