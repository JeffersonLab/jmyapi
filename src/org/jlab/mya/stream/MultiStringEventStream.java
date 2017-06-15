package org.jlab.mya.stream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import org.jlab.mya.EventCode;
import org.jlab.mya.EventStream;
import org.jlab.mya.QueryParams;
import org.jlab.mya.TimeUtil;
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
     * @param params The QueryParams
     * @param con The database connection
     * @param stmt The database statement
     * @param rs The database result set
     */    
    public MultiStringEventStream(QueryParams params, Connection con, PreparedStatement stmt,
            ResultSet rs) {
        super(params, con, stmt, rs);
    }

    @Override
    protected MultiStringEvent rowToEvent() throws SQLException {
        Instant timestamp = TimeUtil.fromMyaTimestamp(rs.getLong(1));
        int codeOrdinal = rs.getInt(2);
        EventCode code = EventCode.values()[codeOrdinal];
        String[] value = new String[params.getMetadata().getSize()];
        int offset = 3;

        for (int i = 0; i < params.getMetadata().getSize(); i++) {
            value[i] = rs.getString(i + offset);
        }
        return new MultiStringEvent(timestamp, code, value);
    }
}
