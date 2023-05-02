package org.jlab.mya.nexus;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import org.jlab.mya.event.EventCode;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.FloatEvent;

/**
 * EventStream for the MySampler algorithm.
 * 
 * Unlike a standard EventStream this one actually creates a new ResultSet for each sample.
 * 
 * @author slominskir
 * @deprecated  use {@link org.jlab.mya.stream.MySamplerStream} instead
 */
@Deprecated
class MySamplerFloatEventStream extends FloatEventStream {

    private Instant begin;
    private long stepMilliseconds;
    private long sampleCount;

    private long sampleCursor = 0;

    /**
     * Create a new BasicSamplerFloatEventStream.
     * 
     * @param params The query parameters
     * @param con The database connection
     * @param stmt The database statement
     */
    public MySamplerFloatEventStream(IntervalQueryParams params, long stepMilliseconds, long sampleCount,
                                     Connection con,
                                     PreparedStatement stmt) {
        super(params, con, stmt, null);
        this.stepMilliseconds = stepMilliseconds;
        this.sampleCount = sampleCount;
        begin = params.getBegin();
    }

    @Override
    public FloatEvent read() throws IOException {
        try {
            if (isOpen()) {
                Instant start = begin.plusMillis(stepMilliseconds
                        * sampleCursor);
                //Instant stop = start.plusMillis(basicParams.getStepMilliseconds());
                stmt.setLong(1, TimeUtil.toMyaTimestamp(start));
                //stmt.setLong(2, TimeUtil.toMyaTimestamp(stop));
                try (ResultSet result = stmt.executeQuery()) {
                    sampleCursor++;
                    
                    if (result.next()) {
                        return rowToEventSingleResultSet(start, result);
                    } else {
                        return new FloatEvent(start, EventCode.UNDEFINED, 0);
                    }
                }
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private FloatEvent rowToEventSingleResultSet(Instant sampleTime, ResultSet result) throws SQLException {
        int codeOrdinal = result.getInt(2);
        EventCode code = EventCode.fromInt(codeOrdinal);
        float value = result.getFloat(3);

        // mySampler treats anything but an UPDATE as UNDEFINED
        if(code != EventCode.UPDATE) {
            code = EventCode.UNDEFINED;
        }
        
        return new FloatEvent(result.getLong(1), code, value);
    }

    @Override
    public boolean isOpen() {
        return sampleCursor < sampleCount;
    }
}
