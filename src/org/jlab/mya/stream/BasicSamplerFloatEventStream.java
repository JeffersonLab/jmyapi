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
 *
 * @author ryans
 */
public class BasicSamplerFloatEventStream extends FloatEventStream {

    private final BasicSamplerParams basicParams;

    private long sampleCursor = 0;

    public BasicSamplerFloatEventStream(BasicSamplerParams params, Connection con,
            PreparedStatement stmt) {
        super(params, con, stmt, null);
        this.basicParams = params;
    }

    @Override
    public FloatEvent read() throws IOException {
        try {
            if (isOpen()) {
                Instant start = params.getBegin().plusMillis(basicParams.getStepMilliseconds()
                        * sampleCursor);
                //Instant stop = start.plusMillis(basicParams.getStepMilliseconds());
                stmt.setLong(1, TimeUtil.toMyaTimestamp(start));
                //stmt.setLong(2, TimeUtil.toMyaTimestamp(stop));
                try (ResultSet result = stmt.executeQuery()) {
                    if (result.next()) {
                        return rowToEventSingleResultSet(start, result);
                    } else {
                        return new FloatEvent(start, EventCode.ORIGIN_OF_CHANNELS_HISTORY, 0);
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
        //Instant timestamp = TimeUtil.fromMyaTimestamp(result.getLong(1));
        int codeOrdinal = result.getInt(2);
        EventCode code = EventCode.values()[codeOrdinal];
        float value = result.getFloat(3);

        sampleCursor++;

        return new FloatEvent(sampleTime, code, value);
    }

    @Override
    public boolean isOpen() {
        return sampleCursor < basicParams.getSampleCount();
    }
}
