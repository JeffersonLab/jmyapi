package org.jlab.mya;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Provides an I/O Channel for streaming Mya events. Since Mya data is often very large it is
 * generally a good idea to stream Mya data instead of accumulate it in memory. Therefore the
 * fundamental behavior of this API is to stream data. You can easily accumulate the data into a
 * collection if you believe the JVM has memory to hold it. Check the count of rows before doing
 * that.
 *
 * A channel remains open until closed so generally you'll want to use a try-with-resources approach
 * to using this class or it's subclasses. Be advised that an open EventStream holds database
 * resources open and should be closed as quickly as possible.
 *
 * This class is abstract and concrete implementations provide Mya event type specific streams.
 *
 * @author slominskir
 * @param <T> The Event type
 */
public abstract class EventStream<T extends Event> implements Channel {

    /**
     * The query parameters.
     */
    protected final QueryParams params;

    /**
     * The database connection.
     */
    protected final Connection con;

    /**
     * The database prepared statement.
     */
    protected final PreparedStatement stmt;

    /**
     * The database result set.
     */
    protected final ResultSet rs;

    /**
     * The type of stream.
     */
    protected final Class<T> type;

    /**
     * Create a new event stream.
     *
     * @param params The query parameters
     * @param con The database connection
     * @param stmt The database statement
     * @param rs The database result set
     * @param type The type
     */
    public EventStream(QueryParams params, Connection con, PreparedStatement stmt, ResultSet rs, Class<T> type) {
        this.params = params;
        this.con = con;
        this.stmt = stmt;
        this.rs = rs;
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
    public T read() throws ClosedChannelException, IOException {
        try {
            if (rs.next()) {
                return rowToEvent();
            } else {
                return null;
            }
        } catch (SQLException e) {
            if(!isOpen()) { // Channel interface says ClosedChannelException specifically should be used
                throw new ClosedChannelException();
            } else {
                throw new IOException(e);
            }
        }
    }

    /**
     * Read the next row of data from the ResultSet and convert it into an event.
     *
     * @return The next event
     * @throws SQLException If unable to convert a ResultSet row to an event.
     */
    protected abstract T rowToEvent() throws SQLException;

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
    };

    /**
     * Tells whether or not this channel is open.
     *
     * @return true if, and only if, this channel is open
     */
    @Override
    public boolean isOpen() {
        try {
            return !rs.isClosed();
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Closes the channel.
     * 
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        try {
            con.close(); // TODO: Make sure closing connection also closes stmt and rs (pooled wrappers I'm looking at you)
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
