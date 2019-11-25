package org.jlab.mya.nexus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.jlab.mya.event.MultiStringEvent;

/**
 * EventStream of MultiStringEvents.
 * 
 * @author slominskir
 */
class MultiStringEventStream extends DatabaseSourceStream<MultiStringEvent> {

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
        super(params, con, stmt, rs, MultiStringEvent.class);
    }

    @Override
    protected MultiStringEvent rowToEvent() throws SQLException {
        return QueryService.fromRow(rs, params.getMetadata().getSize());
    }
}
