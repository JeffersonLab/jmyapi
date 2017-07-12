package org.jlab.mya.stream;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.params.ImprovedSamplerParams;

/**
 * EventStream for the Improved sampler. This stream reads the full dataset from the database and
 * returns a subset (performs application layer filtering).
 *
 * @author ryans
 */
public class ImprovedSamplerFloatEventStream extends FloatEventStream {

    private final ImprovedSamplerParams samplerParams;
    private final long binSize;
    private final BigDecimal fractional;
    private BigDecimal fractionalCounter = BigDecimal.ZERO;

    /**
     * Create a new ImprovedSamplerFloatStream.
     * 
     * @param params The query parameters
     * @param con The database connection
     * @param stmt The database statement
     * @param rs The database result set
     */
    public ImprovedSamplerFloatEventStream(ImprovedSamplerParams params, Connection con,
            PreparedStatement stmt, ResultSet rs) {
        super(params, con, stmt, rs);
        this.samplerParams = params;

        if (params.getCount() > params.getLimit() && params.getLimit() > 0) {
            this.binSize = params.getCount() / params.getLimit();
            this.fractional = BigDecimal.valueOf((params.getCount() % params.getLimit())
                    / Double.valueOf(params.getLimit()));
        } else {
            this.binSize = 1;
            this.fractional = BigDecimal.ZERO;
        }

        //System.out.println("binSize: " + binSize);
        //System.out.println("fractional: " + fractional);
    }

    @Override
    public FloatEvent read() throws IOException {
        FloatEvent event = null;

        BigDecimal old = fractionalCounter;

        //System.out.println("old fractional counter: " + old);
        fractionalCounter = fractionalCounter.add(fractional);

        //System.out.println("new fractional counter: " + fractionalCounter);
        long effectiveBinSize;

        //System.out.println("truncated old: " + old.setScale(0, BigDecimal.ROUND_DOWN));
        //System.out.println("truncated new: " + fractionalCounter.setScale(0, BigDecimal.ROUND_DOWN));
        if (old.setScale(0, BigDecimal.ROUND_DOWN).compareTo(fractionalCounter.setScale(0,
                BigDecimal.ROUND_DOWN)) == 0) {
            effectiveBinSize = binSize;
        } else {
            effectiveBinSize = binSize + 1;
        }

        //System.out.println("effectiveBinSize: " + effectiveBinSize);
        for (int i = 0; i < effectiveBinSize; i++) {
            event = super.read();

            if (event == null) {
                break;
            }
        }

        return event;
    }

}
