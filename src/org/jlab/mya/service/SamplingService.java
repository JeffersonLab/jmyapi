package org.jlab.mya.service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jlab.mya.DataNexus;
import org.jlab.mya.QueryService;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.params.BinnedSamplerParams;
import org.jlab.mya.params.GraphicalSamplerParams;
import org.jlab.mya.params.BasicSamplerParams;
import org.jlab.mya.params.EventSamplerParams;
import org.jlab.mya.stream.GraphicalSamplerFloatEventStream;
import org.jlab.mya.stream.BasicSamplerFloatEventStream;
import org.jlab.mya.stream.FloatEventStream;
import org.jlab.mya.stream.ImprovedSamplerFloatEventStream;

/**
 * Provides query access to the Mya database for a set of sampled events in a
 * given time interval.
 *
 * Sampling is sometimes performed in the database itself as a stored procedure
 * to possibly minimize the amount of data transferred. However, it is possible
 * that sampling will actually increase the size of the result set. This is
 * because Mya data is stored as a set of deltas or changes. There is no fixed
 * update frequency.
 *
 * @author slominskir
 */
public class SamplingService extends QueryService {

    public SamplingService(DataNexus nexus) {
        super(nexus);
    }

    /**
     * Open a stream to float events associated with the specified
     * IntervalQueryParams and sampled using the naive algorithm.
     *
     * The naive algorithm is the what you get with 'myget -l'. "Sampling" is a
     * strong word here since the stored procedure used does not always provide
     * a consistent spacing of data and does not weigh the data. I assume this
     * is comparatively fast vs other sampling methods. It appears the procedure
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
    public FloatEventStream openBinnedSamplerFloatStream(BinnedSamplerParams params) throws
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
     * IntervalQueryParams and sampled using the basic algorithm.
     *
     * The basic algorithm is what you get with 'mySampler'. Each sample is
     * obtained from a separate query. Bins are based on date.
     *
     * Generally you'll want to use try-with-resources around a call to this
     * method to ensure you close the stream properly.
     *
     * @param params The IntervalQueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public FloatEventStream openBasicSamplerFloatStream(BasicSamplerParams params) throws
            SQLException {
        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);
        String query = "select * from table_" + params.getMetadata().getId()
                + " force index for order by (primary) where time <= ? order by time desc limit 1";
        PreparedStatement stmt = con.prepareStatement(query);

        return new BasicSamplerFloatEventStream(params, con, stmt);

    }

    /**
     * Open a stream to float events associated with the specified
     * EventSamplerParams and sampled using the improved algorithm.
     *
     * This algorithm bins by count, not by date interval like many other
     * sampling algorithms. There are pros and cons to this, but it means the
     * event-based nature of the data is preserved and doesn't give periods of
     * calm/idle time as many samples and give more samples to busy activity
     * periods.
     *
     * Another feature of this algorithm is it streams over the entire dataset
     * once instead of issuing n-queries (n = # of bins). There are pros and
     * cons to this as well. This algorithm will generally perform better than
     * issuing n-queries would if there are only a few points per bin.
     *
     * Note: Users must figure out number of events per bin threshold in which
     * to use n-queries instead.
     *
     * @param params The EventSamplerParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public FloatEventStream openEventSamplerFloatStream(EventSamplerParams params) throws
            SQLException {

        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);
        PreparedStatement stmt = nexus.getEventIntervalStatement(con, params);
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getBegin()));
        stmt.setLong(2, TimeUtil.toMyaTimestamp(params.getEnd()));
        ResultSet rs = stmt.executeQuery();
        return new ImprovedSamplerFloatEventStream(params, con, stmt, rs);
    }

    /**
     * Open a stream to float events associated with the specified
     * IntervalQueryParams and sampled using the advanced algorithm.
     *
     * The algorithm used is a modified version of largest triangle three bucket
     * (LTTB) described in "Downsampling Time Series for Visual Representation"
     * (Steinarsson, 2013). In addition to determining the LTTB point for this
     * bucket, it also collects and non-update events, the minimum event by
     * value, and the maximum event by value. This is an online algorithm and
     * does not persist data.
     *
     * Generally you'll want to use try-with-resources around a call to this
     * method to ensure you close the stream properly.
     *
     * @param params The IntervalQueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public FloatEventStream openGraphicalSamplerFloatStream(GraphicalSamplerParams params) throws
            SQLException {

        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);
        PreparedStatement stmt = nexus.getEventIntervalStatement(con, params);
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getBegin()));
        stmt.setLong(2, TimeUtil.toMyaTimestamp(params.getEnd()));
        ResultSet rs = stmt.executeQuery();
        return new GraphicalSamplerFloatEventStream(params, con, stmt, rs);
    }
}
