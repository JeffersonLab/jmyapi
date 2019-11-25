package org.jlab.mya.stream;

import org.jlab.mya.event.Event;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;

/**
 * Provides an I/O Channel for streaming Mya events. Since Mya data is often very large it is
 * generally a good idea to stream Mya data instead of accumulate it in memory. Therefore the
 * fundamental behavior of this API is to stream data. You can easily accumulate the data into a
 * collection if you believe the JVM has memory to hold it. Check the count of rows before doing
 * that.
 *
 * A channel remains open until closed so generally you'll want to use a try-with-resources approach
 * to using this class or it's subclasses. Be advised that generally an open EventStream holds database
 * resources open and should be closed as quickly as possible (generally the first stream in a set of possibly nested
 * streams is a DatabaseSourceStream, which holds a database connection).
 *
 * This class is abstract and concrete implementations provide Mya event type specific streams.
 *
 * @author slominskir
 * @param <T> The Event type
 */
public abstract class EventStream<T extends Event> implements Channel {

    /**
     * The type of stream.
     */
    private final Class<T> type;

    /**
     * Create a new event stream.
     *
     * @param type The type
     */
    protected EventStream(Class<T> type) {
        this.type = type;
    }

    /**
     * Read the next event from the stream. Generally you'll want to iterate over the stream using a
     * while loop.
     *
     * @return The next event or null if End-Of-Stream reached
     * @throws ClosedChannelException If the channel is closed
     * @throws IOException If unable to read the next event
     */
    public abstract T read() throws ClosedChannelException, IOException;

    /**
     * Obtain type of stream.
     * <p>
     * In Java, Generics are erased at runtime so if you want to know what type something is
     * you must pass it as an argument.  That's why type info is specified twice in EventStream: once for the compiler
     * and once for the runtime.   All so you can call the method getType() at runtime!
     * </p>
     *
     * @return The type
     */
    public Class<T> getType() {
        return type;
    }
}
