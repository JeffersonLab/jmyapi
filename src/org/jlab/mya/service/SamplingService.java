package org.jlab.mya.service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jlab.mya.DataNexus;
import org.jlab.mya.QueryParams;
import org.jlab.mya.QueryService;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.params.NaiveSamplerParams;
import org.jlab.mya.stream.FloatEventStream;

/**
 * Provides sampled event interval query access to the Mya database.
 *
 * Sampling is sometimes performed in the database itself as a stored procedure to possibly minimize
 * the amount of data transferred. However, it is possible that sampling will actually increase the
 * size of the result set. This is because Mya data is stored as a set of deltas or changes. There
 * is no fixed update frequency.
 *
 * @author slominskir
 */
public class SamplingService extends QueryService {

    public SamplingService(DataNexus nexus) {
        super(nexus);
    }

    /**
     * Open a stream to float events associated with the specified QueryParams and sampled using the
     * naive algorithm.
     *
     * The naive algorithm is the what you get with 'myget -l'. "Sampling" is a strong word here
     * since the stored procedure used does not always provide a consistent spacing of data and does
     * not weigh the data. I assume this is comparatively fast vs other sampling methods. It appears
     * the procedure simply divides the interval into sub-intervals based on the number of requested
     * points and then queries for the first event in each sub-interval. If no event is found in the
     * sub-interval then no point is provided for that sub-interval meaning that the user may get
     * less points then the limit.
     *
     * Generally you'll want to use try-with-resources around a call to this method to ensure you
     * close the stream properly.
     *
     * @param params The QueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public FloatEventStream openFloatNaiveSampler(NaiveSamplerParams params) throws SQLException {
        long maxPoints = params.getLimit();

        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);

        // Note: If we wanted the statment to be reused we would need to move it to DataNexus to allow implentations the opportunity to cache.
        // Note: Call can return a result set, but C++ version doesn't so we don't; I guess for performance reasons?
        // Note: There are two other procedures "Bin" and "reduce", but neither appears to be used by C++ myapi.
        // Bin appears to take an average for each sub-interval instead of simply taking the first value.
        // reduce takes every nth event instead of breaking down the interval into sub-intervals
        String query = "{call Sample(?, ?, ?, ?, ?)}";
        CallableStatement stmtA = con.prepareCall(query);

        stmtA.setString(1, "table_" + params.getMetadata().getId());
        stmtA.setLong(2, maxPoints);
        stmtA.setLong(3, TimeUtil.toMyaTimestamp(params.getBegin()));
        stmtA.setLong(4, TimeUtil.toMyaTimestamp(params.getEnd()));
        stmtA.setInt(5, 0); // if zero then don't return anything.   If non-zero then return result set and clean up temporary table.  We're currently do this the hard way by not returning result set...
        stmtA.execute();
        stmtA.close();

        query = "select * from table_x order by time";
        PreparedStatement stmtB = con.prepareStatement(query);
        ResultSet rs = stmtB.executeQuery();

        // TODO: what the heck do we do with stmtC ... can't delete tmp table until results are consumed.... however closing connection will delete automatically for now....
        //query = "drop temporary table if exists table_x";
        //PreparedStatement stmtC = con.prepareStatement(query);        
        return new FloatEventStream(params, con, stmtB, rs);
    }

    /**
     * Open a stream to float events associated with the specified QueryParams and sampled using the
     * basic algorithm (not supported yet).
     *
     * The basic algorithm is what you get with 'mySampler'.
     *
     * Generally you'll want to use try-with-resources around a call to this method to ensure you
     * close the stream properly.
     *
     * @param params The QueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public FloatEventStream openFloatBasicSampler(QueryParams params) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
        // See archive.cpp Snapshot
    }

    /**
     * Open a stream to float events associated with the specified QueryParams and sampled using the
     * advanced algorithm (not supported yet).
     *
     * TODO: Describe advanced algorithm.
     *
     * Generally you'll want to use try-with-resources around a call to this method to ensure you
     * close the stream properly.
     *
     * @param params The QueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public FloatEventStream openFloatAdvancedSampler(QueryParams params) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
