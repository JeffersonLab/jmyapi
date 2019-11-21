package org.jlab.mya.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.jlab.mya.*;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.stream.FloatEventStream;
import org.jlab.mya.stream.IntEventStream;
import org.jlab.mya.stream.MultiStringEventStream;
import org.jlab.mya.DataNexus;
import org.jlab.mya.EventStream;
import org.jlab.mya.QueryService;
import org.jlab.mya.TimeUtil;

/**
 * Provides query access to the Mya database for a set of events in a given time
 * interval.
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
     * Count the number of events associated with the supplied
     * IntervalQueryParams.
     *
     * @param params The IntervalQueryParams
     * @return The number of events
     * @throws SQLException If unable to query the database
     */
    public long count(IntervalQueryParams params) throws SQLException {
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
     * Open a stream to events associated with the specified
     * IntervalQueryParams. This method returns a generic stream which will need
     * to be cast to obtain values (values may be primitives so a
     * generic getValue() method returning a generic would not work unless you
     * want to accept primitive autoboxing performance cost).
     *
     * Generally you'll want to use try-with-resources around a call to this
     * method to ensure you close the stream properly.
     *
     * @param params The IntervalQueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public EventStream openEventStream(IntervalQueryParams params) throws SQLException {
        EventStream stream;

        if (params.getMetadata().getSize() > 1) {
            stream = openMultiStringStream(params);
        } else {
            switch (params.getMetadata().getType()) {
                case DBR_SHORT:
                case DBR_LONG:
                case DBR_ENUM:
                    stream = openIntStream(params);
                    break;
                case DBR_FLOAT:
                case DBR_DOUBLE:
                    stream = openFloatStream(params);
                    break;
                default:
                    stream = openMultiStringStream(params);
                    break;
            }
        }

        return stream;
    }

    /**
     * Open a stream to float-valued events associated with the specified
     * IntervalQueryParams.
     *
     * Generally you'll want to use try-with-resources around a call to this
     * method to ensure you close the stream properly.
     *
     * @param params The IntervalQueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public FloatEventStream openFloatStream(IntervalQueryParams params) throws SQLException {
        InternalIntervalParams iip = openStream(params);

        return new FloatEventStream(params, iip.con, iip.stmt, iip.rs);
    }

    /**
     * Open a stream to int-valued events associated with the specified
     * IntervalQueryParams.
     *
     * Generally you'll want to use try-with-resources around a call to this
     * method to ensure you close the stream properly.
     *
     * @param params The IntervalQueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public IntEventStream openIntStream(IntervalQueryParams params) throws SQLException {
        InternalIntervalParams iip = openStream(params);

        return new IntEventStream(params, iip.con, iip.stmt, iip.rs);
    }

    /**
     * Open a stream to multi-string-valued events associated with the specified
     * IntervalQueryParams.
     *
     * Generally you'll want to use try-with-resources around a call to this
     * method to ensure you close the stream properly.
     *
     * @param params The IntervalQueryParams
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public MultiStringEventStream openMultiStringStream(IntervalQueryParams params) throws
            SQLException {
        InternalIntervalParams iip = openStream(params);

        return new MultiStringEventStream(params, iip.con, iip.stmt, iip.rs);
    }

    /**
     * Internal shared method for opening a stream (regardless of type) and bundling parameters.
     *
     * @param params The user specified interval query parameters
     * @return The internal bundled stream parameters
     * @throws SQLException If unable to open a stream
     */
    private InternalIntervalParams openStream(IntervalQueryParams params) throws SQLException {
        InternalIntervalParams iip = new InternalIntervalParams();

        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);
        int fetchSize = 0;
        if(params.getFetchStrategy() == IntervalQueryParams.IntervalQueryFetchStrategy.STREAM) {
            fetchSize = Integer.MIN_VALUE;
        } else if(params.getFetchStrategy() == IntervalQueryParams.IntervalQueryFetchStrategy.CHUNK) {
            fetchSize = 4098;
            ((com.mysql.jdbc.Connection)con).setUseCursorFetch(true);
        }
        PreparedStatement stmt = nexus.getEventIntervalStatement(con, params, fetchSize);
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getBegin()));
        stmt.setLong(2, TimeUtil.toMyaTimestamp(params.getEnd()));
        ResultSet rs = stmt.executeQuery();

        if(params.getFetchStrategy() == IntervalQueryParams.IntervalQueryFetchStrategy.CHUNK) {
            ((com.mysql.jdbc.Connection)con).setUseCursorFetch(false);
        }

        iip.con = con;
        iip.stmt = stmt;
        iip.rs = rs;

        return iip;
    }

    /**
     * A common bundle of parameters used internally to avoid duplicating code.
     */
    private class InternalIntervalParams {
        public Connection con;
        public PreparedStatement stmt;
        public ResultSet rs;
    }
}
