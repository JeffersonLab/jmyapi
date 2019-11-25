package org.jlab.mya.stream;

import org.jlab.mya.event.Event;

import java.nio.channels.ClosedChannelException;
import java.util.List;

/**
 * An EventStream backed by an in-memory list.  This is useful for small sets of data and testing.
 *
 * @param <T> The Event type
 */
public class ListStream<T extends Event> extends EventStream<T> {
    private int i = 0;
    private final List<T> events;
    private boolean open = true;

    /**
     * Create a new ListStream.
     *
     * @param events The events that make up the stream
     * @param type The type
     */
    public ListStream(List<T> events, Class<T> type) {
        super(type);
        this.events = events;
    }

    @Override
    public T read() throws ClosedChannelException {
        if(!open) {
            throw new ClosedChannelException(); // java.nio.channels.Channel contract says we must
        }

        if(i < events.size()) {
            return events.get(i++);
        } else {
            return null;
        }
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
    }
}
