package org.jlab.mya.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jlab.mya.DataNexus;
import org.jlab.mya.QueryParams;
import org.jlab.mya.QueryService;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.stream.FloatEventStream;
import org.jlab.mya.stream.IntEventStream;
import org.jlab.mya.stream.MultiStringEventStream;

/**
 * Provides event interval query access to the Mya database.
 *
 * @author slominskir
 */
public class IntervalService extends QueryService {

    /**
     * Create a new QueryService with the provided DataNexus.
     *
     * @param nexus The DataNexus
     */
    public IntervalService(DataNexus nexus) {
        super(nexus);
    }

    /**
     * Count the number of events associated with the supplied QueryParams.
     *
     * @param params The QueryParams
     * @return The number of events
     * @throws SQLException If unable to query the database
     */
    public long count(QueryParams params) throws SQLException {
        long count;
        String host = params.getMetadata().getHost();
        try (Connection con = nexus.getConnection(host)) {
            try (PreparedStatement stmt = nexus.getCountStatement(con, params)) {
                stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getBegin()));
                stmt.setLong(2, TimeUtil.toMyaTimestamp(params.getEnd()));

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        count = rs.getLong(1);
                    } else {
                        throw new SQLException("Unable to count records in table_"
                                + params.getMetadata().getId());
                    }
                }
            }
        }

        return count;
    }

    /**
     * Open a stream to float events associated with the specified QueryParams.
     *
     * Generally you'll want to use try-with-resources around a call to this method to ensure you
     * close the stream properly.
     *
     * @param params The QueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public FloatEventStream openFloatStream(QueryParams params) throws SQLException {
        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);
        PreparedStatement stmt = nexus.getEventStatement(con, params);
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getBegin()));
        stmt.setLong(2, TimeUtil.toMyaTimestamp(params.getEnd()));
        ResultSet rs = stmt.executeQuery();
        return new FloatEventStream(params, con, stmt, rs);
    }

    /**
     * Open a stream to int events associated with the specified QueryParams.
     *
     * Generally you'll want to use try-with-resources around a call to this method to ensure you
     * close the stream properly.
     *
     * @param params The QueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public IntEventStream openIntStream(QueryParams params) throws SQLException {
        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);
        PreparedStatement stmt = nexus.getEventStatement(con, params);
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getBegin()));
        stmt.setLong(2, TimeUtil.toMyaTimestamp(params.getEnd()));
        ResultSet rs = stmt.executeQuery();
        return new IntEventStream(params, con, stmt, rs);
    }

    /**
     * Open a stream to multi string events associated with the specified QueryParams.
     *
     * Generally you'll want to use try-with-resources around a call to this method to ensure you
     * close the stream properly.
     *
     * @param params The QueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public MultiStringEventStream openMultiStringStream(QueryParams params) throws SQLException {
        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);
        PreparedStatement stmt = nexus.getEventStatement(con, params);
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getBegin()));
        stmt.setLong(2, TimeUtil.toMyaTimestamp(params.getEnd()));
        ResultSet rs = stmt.executeQuery();
        return new MultiStringEventStream(params, con, stmt, rs);
    }
}
