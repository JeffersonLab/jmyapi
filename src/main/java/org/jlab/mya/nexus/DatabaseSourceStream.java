package org.jlab.mya.nexus;

import org.jlab.mya.Event;
import org.jlab.mya.EventStream;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Provides an I/O Channel for streaming Mya events out of the source MySQL database.
 *
 * This class is abstract and concrete implementations provide Mya event type specific streams.
 *
 * @author slominskir
 * @param <T> The Event type
 */
abstract class DatabaseSourceStream<T extends Event> extends EventStream<T> {
    /**
     * The query parameters.
     */
    protected final QueryParams params;

    /**
     * The database connection.
     */
    private final Connection con;

    /**
     * The database prepared statement.
     */
    protected final PreparedStatement stmt;

    /**
     * The database result set.
     */
    protected final ResultSet rs;

    /**
     * Create a new event stream.
     *
     * @param params The query parameters
     * @param con    The database connection
     * @param stmt   The database statement
     * @param rs     The database result set
     * @param type   The type
     */
    protected DatabaseSourceStream(QueryParams params, Connection con, PreparedStatement stmt, ResultSet rs, Class<T> type) {
        super(type);
        this.params = params;
        this.con = con;
        this.stmt = stmt;
        this.rs = rs;
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
