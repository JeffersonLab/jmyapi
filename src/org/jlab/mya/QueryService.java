package org.jlab.mya;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jlab.mya.stream.FloatEventStream;
import org.jlab.mya.stream.IntEventStream;
import org.jlab.mya.stream.MultiStringEventStream;

/**
 * Provides query access to the Mya database.
 * 
 * @author slominskir
 */
public class QueryService {

    private final DataNexus nexus;

    /**
     * Create a new QueryService with the provided DataNexus.
     * 
     * @param nexus The DataNexus
     */
    public QueryService(DataNexus nexus) {
        this.nexus = nexus;
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
     * Query for PV metadata given PV name.
     * 
     * @param name The PV name
     * @return PV metadata
     * @throws SQLException If unable to query the database 
     */
    public Metadata findMetadata(String name) throws SQLException {
        Metadata metadata;

        String master = nexus.getMasterHostName();
        try (Connection con = nexus.getConnection(master)) {
            try (PreparedStatement stmt = nexus.getMetadataStatement(con)) {

                stmt.setString(1, name);

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int id = rs.getInt("chan_id");
                        String host = rs.getString("host");
                        int typeOrdinal = rs.getInt("type");
                        int size = rs.getInt("size");

                        DataType type = DataType.values()[typeOrdinal];

                        metadata = new Metadata(id, name, host, type, size);
                    } else {
                        metadata = null;
                    }
                }
            }
        }

        return metadata;
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
    public FloatEventStream openFloat(QueryParams params) throws SQLException {
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
    public IntEventStream openInt(QueryParams params) throws SQLException {
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
    public MultiStringEventStream openMultiString(QueryParams params) throws SQLException {
        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);
        PreparedStatement stmt = nexus.getEventStatement(con, params);
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getBegin()));
        stmt.setLong(2, TimeUtil.toMyaTimestamp(params.getEnd()));
        ResultSet rs = stmt.executeQuery();
        return new MultiStringEventStream(params, con, stmt, rs);
    }

    /**
     * Return the DataNexus.
     * 
     * @return The DataNexus
     */
    public DataNexus getNexus() {
        return nexus;
    }
}
