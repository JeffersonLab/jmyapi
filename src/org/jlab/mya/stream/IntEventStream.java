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
import org.jlab.mya.event.IntEvent;

/**
 *
 * @author slominskir
 */
public class IntEventStream extends EventStream<IntEvent> {

    public IntEventStream(QueryParams params, Connection con, PreparedStatement stmt, ResultSet rs) {
        super(params, con, stmt, rs);
    }

    @Override
    protected IntEvent rowToEvent() throws SQLException {
        Instant timestamp = TimeUtil.fromMyaTimestamp(rs.getLong(1));
        int codeOrdinal = rs.getInt(2);
        EventCode code = EventCode.values()[codeOrdinal];
        int value = rs.getInt(3);
        return new IntEvent(timestamp, code, value);
    }
}
