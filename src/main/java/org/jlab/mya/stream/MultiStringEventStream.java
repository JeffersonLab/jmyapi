package org.jlab.mya.stream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jlab.mya.EventStream;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.event.MultiStringEvent;

/**
 * EventStream of MultiStringEvents.
 * 
 * @author slominskir
 */
public class MultiStringEventStream extends EventStream<MultiStringEvent> {

    /**
     * Create a new MultiStringEventStream.
     * 
     * @param params The IntervalQueryParams
     * @param con The database connection
     * @param stmt The database statement
     * @param rs The database result set
     */    
    public MultiStringEventStream(IntervalQueryParams params, Connection con, PreparedStatement stmt,
            ResultSet rs) {
        super(params, con, stmt, rs);
    }

    @Override
    protected MultiStringEvent rowToEvent() throws SQLException {
        return MultiStringEvent.fromRow(rs, params.getMetadata().getSize());
    }
}
