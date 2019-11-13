package org.jlab.mya.stream;

import org.jlab.mya.Event;
import org.jlab.mya.EventStream;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * An EventStream backed by an in-memory list.  This is useful for small sets of data and testing.
 *
 * @param <T> The Event type
 */
public class ListStream<T extends Event> extends EventStream<T> {
    private int i = 0;
    private final List<T> events;

    public ListStream(List<T> events) {
        super(null, null, null, null);
        this.events = events;
    }

    @Override
    protected T rowToEvent() throws SQLException {
        return null;
    }

    @Override
    public T read() throws IOException {
        if(i < events.size()) {
            return events.get(i++);
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        // No op
    }
}
