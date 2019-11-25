package org.jlab.mya.stream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.event.FloatEvent;

/**
 * EventStream of FloatEvents.
 * 
 * @author slominskir
 */
public class FloatEventStream extends DatabaseSourceStream<FloatEvent> {

    /**
     * Create a new FloatEventStream.
     * 
     * @param params The IntervalQueryParams
     * @param con The database connection
     * @param stmt The database statement
     * @param rs The database result set
     */
    public FloatEventStream(IntervalQueryParams params, Connection con, PreparedStatement stmt, ResultSet rs) {
        super(params, con, stmt, rs, FloatEvent.class);
    }

    @Override
    protected FloatEvent rowToEvent() throws SQLException {
        return FloatEvent.fromRow(rs);
    }
}
