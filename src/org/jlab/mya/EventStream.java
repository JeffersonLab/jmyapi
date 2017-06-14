package org.jlab.mya;

import java.io.IOException;
import java.nio.channels.Channel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author ryans
 * @param <T>
 */
public abstract class EventStream<T> implements Channel {

    protected final QueryParams params;
    protected final Connection con;
    protected final PreparedStatement stmt;
    protected final ResultSet rs;

    public EventStream(QueryParams params, Connection con, PreparedStatement stmt, ResultSet rs) {
        this.params = params;
        this.con = con;
        this.stmt = stmt;
        this.rs = rs;
    }

    public T read() throws IOException {
        try {
            if (rs.next()) {
                return rowToEntity();
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    protected abstract T rowToEntity() throws SQLException;

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
