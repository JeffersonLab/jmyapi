package org.jlab.mya.stream;

import org.jlab.mya.Event;
import org.jlab.mya.EventStream;

import java.util.List;

/**
 * An EventStream backed by an in-memory list.  This is useful for small sets of data and testing.
 *
 * @param <T> The Event type
 */
public class ListStream<T extends Event> extends EventStream<T> {
    private int i = 0;
    private final List<T> events;

    /**
     * Create a new ListStream.
     *
     * @param events The events that make up the stream
     * @param type The type
     */
    public ListStream(List<T> events, Class<T> type) {
        super(null, null, null, null, type);
        this.events = events;
    }

    @Override
    protected T rowToEvent() {
        return null;
    }

    @Override
    public T read() {
        if(i < events.size()) {
            return events.get(i++);
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        // No op
    }
}
