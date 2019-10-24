package org.jlab.mya.service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jlab.mya.DataNexus;
import org.jlab.mya.QueryService;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.params.MyGetSampleParams;
import org.jlab.mya.params.MySamplerParams;
import org.jlab.mya.stream.MySamplerFloatEventStream;
import org.jlab.mya.stream.FloatEventStream;

/**
 * Provides query access to the Mya database for a set of source (in-database) sampled events in a
 * given time interval.  In-database sampling is generally faster than sampling at the application level.  However,
 * it can be less flexible (if you want to manipulate the full data set before sampling).
 *
 * Application sampling is available by using classes in the stream.wrapped package.
 *
 * Source sampling is sampling performed in the database itself (generally as a stored procedure)
 * to possibly minimize the amount of data transferred. However, it is possible
 * that sampling will actually increase the size of the result set. This is
 * because Mya data is stored as a set of deltas or changes. There is no fixed
 * update frequency.
 *
 * @author slominskir
 */
public class SourceSamplingService extends QueryService {

    public SourceSamplingService(DataNexus nexus) {
        super(nexus);
    }

    /**
     * Open a stream to float events associated with the specified
     * IntervalQueryParams and sampled using the naive algorithm.
     *
     * The myget algorithm is the what you get with MYA 'myget -l'. A stored procedure is used,
     * and does not always provide
     * a consistent spacing of data and does not weigh the data. This approach is generally comparatively fast
     * vs other sampling methods. It appears the procedure
     * simply divides the interval into sub-intervals based on the number of
     * requested points and then queries for the first event in each
     * sub-interval. If no event is found in the sub-interval then no point is
     * provided for that sub-interval meaning that the user may get less points
     * then the limit. Sub-intervals are spaced by date.
     *
     * Generally you'll want to use try-with-resources around a call to this
     * method to ensure you close the stream properly.
     *
     * Note: Implemented using a database stored procedure.
     *
     * @param params The IntervalQueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public FloatEventStream openMyGetSampleFloatStream(MyGetSampleParams params) throws
            SQLException {
        long maxPoints = params.getLimit();

        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);

        // Note: If we wanted the statement to be reused we would need to move it to DataNexus to allow implementations the opportunity to cache.
        // Note: Call can return a result set, but C++ version doesn't so we don't; I guess for performance reasons?
        // Note: There are two other procedures "Bin" and "reduce", but neither appears to be used by C++ myapi.
        // Bin appears to take an average for each sub-interval instead of simply taking the first value.
        // reduce takes every nth event instead of breaking down the interval into time-based sub-intervals
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

        // Note: We do not do anything with stmtC ... can't delete tmp table until results are consumed.... however closing connection will delete automatically for now....
        //query = "drop temporary table if exists table_x";
        //PreparedStatement stmtC = con.prepareStatement(query);        
        return new FloatEventStream(params, con, stmtB, rs);
    }

    /**
     * Open a stream to float events associated with the specified
     * IntervalQueryParams and sampled using the mySampler algorithm.
     *
     * The mySampler algorithm is what you get with MYA 'mySampler'. Each sample is
     * obtained from a separate query. Bins are based on date.
     *
     * Generally you'll want to use try-with-resources around a call to this
     * method to ensure you close the stream properly.
     *
     * @param params The IntervalQueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public FloatEventStream openMySamplerFloatStream(MySamplerParams params) throws
            SQLException {
        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);
        String query = "select * from table_" + params.getMetadata().getId()
                + " force index for order by (primary) where time <= ? order by time desc limit 1";
        PreparedStatement stmt = con.prepareStatement(query);

        return new MySamplerFloatEventStream(params, con, stmt);

    }
}
