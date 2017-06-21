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
import org.jlab.mya.event.FloatEvent;

/**
 * EventStream of FloatEvents.
 * 
 * @author slominskir
 */
public class FloatEventStream extends EventStream<FloatEvent> {

    /**
     * Create a new FloatEventStream.
     * 
     * @param params The QueryParams
     * @param con The database connection
     * @param stmt The database statement
     * @param rs The database result set
     */
    public FloatEventStream(QueryParams params, Connection con, PreparedStatement stmt, ResultSet rs) {
        super(params, con, stmt, rs);
    }

    @Override
    protected FloatEvent rowToEvent() throws SQLException {
        Instant timestamp = TimeUtil.fromMyaTimestamp(rs.getLong(1));
        int codeOrdinal = rs.getInt(2);
        EventCode code = EventCode.fromInt(codeOrdinal);
        float value = rs.getFloat(3);
        return new FloatEvent(timestamp, code, value);
    }
}
