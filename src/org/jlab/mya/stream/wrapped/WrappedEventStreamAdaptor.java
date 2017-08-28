package org.jlab.mya.stream.wrapped;

import java.io.IOException;
import java.sql.SQLException;
import org.jlab.mya.Event;
import org.jlab.mya.EventStream;

/**
 * Adaptor for classes wishing to wrap an EventStream. The Event Type may be
 * different between the outer and inner streams.
 *
 * @author slominskir
 * @param <T> The Outer Event Type
 * @param <E> The Inner (Wrapped) Event Type
 */
public abstract class WrappedEventStreamAdaptor<T extends Event, E extends Event> extends EventStream<T> {

    protected final EventStream<E> wrapped;

    /**
     * Create a new WrappedEventStreamAdaptor.
     * 
     * @param wrapped The wrapped EventStream
     */
    public WrappedEventStreamAdaptor(EventStream<E> wrapped) {
        super(null, null, null, null);
        this.wrapped = wrapped;
    }

    @Override
    protected T rowToEvent() throws SQLException {
        return null;
    }

    /**
     * Read the next event from the stream. Generally you'll want to iterate
     * over the stream using a while loop.
     *
     * @return The next event or null if End-Of-Stream reached
     * @throws IOException If unable to read the next event
     */
    @Override
    public T read() throws IOException {
        return null;
    }

    /**
     * Tells whether or not this channel is open.
     *
     * @return true if, and only if, this channel is open
     */
    @Override
    public boolean isOpen() {
        return wrapped.isOpen();
    }

    /**
     * Closes the channel.
     *
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        wrapped.close();
    }
}
