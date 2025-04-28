package events;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseEvent {
    protected Long timestamp;
    protected final Map<String, Object> attributes;

    public BaseEvent(Long timestamp) {
        this.timestamp = timestamp;
        this.attributes = new HashMap<>();
    }

    // Add attribute to the event
    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    // Retrieve timestamp
    public Long getTimestamp() {
        return timestamp;
    }

    // Get attribute by key
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    // Abstract method for subclasses to implement custom mapping
    public abstract Map<String, Object> toMap();

    public Map<String, Object> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    @Override
    public String toString() {
        return "BaseEvent{" +
                "timestamp=" + timestamp +
                ", attributes=" + attributes +
                '}';
    }
}