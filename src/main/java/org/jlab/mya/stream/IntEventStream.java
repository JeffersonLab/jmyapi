package org.jlab.mya.stream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jlab.mya.EventStream;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.event.IntEvent;

/**
 * EventStream of IntEvents.
 * 
 * @author slominskir
 */
public class IntEventStream extends EventStream<IntEvent> {

    /**
     * Create a new IntEventStream.
     * 
     * @param params The IntervalQueryParams
     * @param con The database connection
     * @param stmt The database statement
     * @param rs The database result set
     */    
    public IntEventStream(IntervalQueryParams params, Connection con, PreparedStatement stmt, ResultSet rs) {
        super(params, con, stmt, rs);
    }

    @Override
    protected IntEvent rowToEvent() throws SQLException {
        return IntEvent.fromRow(rs);
    }
}
