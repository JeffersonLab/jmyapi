package org.jlab.mya.stream;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import org.jlab.mya.EventCode;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.params.BasicSamplerParams;

/**
 * EventStream for the Basic sampler.
 * 
 * Unlike a standard EventStream this one actually creates a new ResultSet for each sample.
 * 
 * @author slominskir
 */
public class BasicSamplerFloatEventStream extends FloatEventStream {

    private final BasicSamplerParams basicParams;

    private long sampleCursor = 0;

    /**
     * Create a new BasicSamplerFloatEventStream.
     * 
     * @param params The query parameters
     * @param con The database connection
     * @param stmt The database statement
     */
    public BasicSamplerFloatEventStream(BasicSamplerParams params, Connection con,
            PreparedStatement stmt) {
        super(params, con, stmt, null);
        this.basicParams = params;
    }

    @Override
    public FloatEvent read() throws IOException {
        try {
            if (isOpen()) {
                Instant start = basicParams.getBegin().plusMillis(basicParams.getStepMilliseconds()
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

    protected FloatEvent rowToEventSingleResultSet(Instant sampleTime, ResultSet result) throws SQLException {
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
        return sampleCursor < basicParams.getSampleCount();
    }
}
