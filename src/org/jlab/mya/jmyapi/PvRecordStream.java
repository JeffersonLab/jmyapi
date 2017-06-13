package org.jlab.mya.jmyapi;

import java.io.IOException;
import java.nio.channels.Channel;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;

/**
 *
 * @author ryans
 */
public class PvRecordStream implements Channel {

    private final Statement stmt;
    private final ResultSet rs;

    protected PvRecordStream(Statement stmt, ResultSet rs) {
        this.stmt = stmt;
        this.rs = rs;
    }

    @Override
    public boolean isOpen() {
        try {
            return !rs.isClosed();
        } catch (SQLException e) {
            return false;
        }
    }

    public PvRecord read() throws IOException {
        try {
            if (rs.next()) {
                return rowToEntity();
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new IOException("Unable to read", e);
        }
    }

    private PvRecord rowToEntity() throws SQLException {
        Instant time = MyaUtil.fromMyaTimestamp(rs.getLong(1));
        int codeOrdinal = rs.getInt(2);
        
        PvEventType code = PvEventType.values()[codeOrdinal];

        return new PvRecord(time, code);
    }

    @Override
    public void close() throws IOException {
        try {
            //rs.close();
            stmt.close();
            /*Closing Statement closes the ResultSet too*/
        } catch (SQLException e) {
            throw new IOException("Unable to close SpanOutputChannel", e);
        }
    }

    /*public Iterator<PvRecord> iterator() {
        // Don't mix iterator and read calls or bad things happen...
        return new Iterator<PvRecord>() {
            private boolean didNext = false;
            private boolean hasNext = false;

            @Override
            public boolean hasNext() {
                if (!didNext) {
                    try {
                        hasNext = rs.next();
                    } catch (SQLException e) {
                        throw new RuntimeException("Unable to iterate", e);
                    }
                    didNext = true;
                }
                return hasNext;
            }

            @Override
            public PvRecord next() {
                try {
                    if (!didNext) {
                        rs.next();
                    }
                    didNext = true;
                    return rowToEntity();
                } catch (SQLException e) {
                    throw new RuntimeException("Unable to iterate", e);
                }
            }
        };
    }*/
}
