package org.jlab.mya;

import java.io.IOException;
import java.nio.channels.Channel;
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
 * @author ryans
 * @param <T> The Event type
 */
public abstract class EventStream<T extends Event> implements Channel {

    protected final QueryParams params;
    protected final Connection con;
    protected final PreparedStatement stmt;
    protected final ResultSet rs;

    /**
     * Create a new event stream.
     * 
     * @param params The query parameters
     * @param con The database connection
     * @param stmt The database statement
     * @param rs The database result set
     */
    public EventStream(QueryParams params, Connection con, PreparedStatement stmt, ResultSet rs) {
        this.params = params;
        this.con = con;
        this.stmt = stmt;
        this.rs = rs;
    }

    /**
     * Read the next event from the stream.  Generally you'll want to iterate over the stream using 
     * a while loop.
     * 
     * @return The next event or null if End-Of-Stream reached
     * @throws IOException If unable to read the next event
     */
    public T read() throws IOException {
        try {
            if (rs.next()) {
                return rowToEvent();
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    /**
     * Read the next row of data from the ResultSet and convert it into an event.
     * 
     * @return The next event
     * @throws SQLException If unable to convert a ResultSet row to an event.
     */
    protected abstract T rowToEvent() throws SQLException;

    @Override
    public boolean isOpen() {
        try {
            return !rs.isClosed();
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            con.close(); // TODO: Make sure closing connection also closes stmt and rs (pooled wrappers I'm looking at you)
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
