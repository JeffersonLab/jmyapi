package org.jlab.mya.stream.wrapped;

import java.io.IOException;

import org.jlab.mya.Event;
import org.jlab.mya.EventStream;

/**
 * An EventStream that wraps another EventStream. The Event Type may be
 * different between the outer and inner streams.
 *
 * @author slominskir
 * @param <T> The Outer Event Type
 * @param <E> The Inner (Wrapped) Event Type
 */
public abstract class WrappedStream<T extends Event, E extends Event> extends EventStream<T> {

    final EventStream<E> wrapped;

    /**
     * Create a new WrappedEventStreamAdaptor.
     * 
     * @param wrapped The wrapped EventStream
     * @param type The type
     */
    WrappedStream(EventStream<E> wrapped, Class<T> type) {
        super(type);
        this.wrapped = wrapped;
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
