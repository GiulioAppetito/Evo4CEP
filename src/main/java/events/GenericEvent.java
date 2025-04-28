package events;

import java.util.HashMap;
import java.util.Map;

public class GenericEvent extends BaseEvent {
    public GenericEvent(Long timestamp) {
        super(timestamp);
    }

    @Override
    public Map<String, Object> toMap() {
        // Create a Map and copy existing attributes
        Map<String, Object> map = new HashMap<>(super.getAttributes());
        map.put("timestamp", timestamp);
        return map;
    }


    @Override
    public String toString() {
        return "GenericEvent{" +
                "timestamp=" + timestamp +
                ", attributes=" + attributes +
                '}';
    }
}
