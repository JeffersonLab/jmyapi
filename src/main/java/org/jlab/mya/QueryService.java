package org.jlab.mya;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

/**
 * Provides query access to the Mya database.
 *
 * This class is abstract because it simply sets up the framework for
 * specialized services to do the heavy lifting. This base class houses the
 * DataNexus and Metadata lookup, both of which are required for any specialized
 * query service to function.
 *
 * @author slominskir
 */
public abstract class QueryService {

    /**
     * The DataNexus, which is the gateway to querying a Mya cluster.
     */
    protected final DataNexus nexus;

    /**
     * Create a new QueryService with the provided DataNexus.
     *
     * @param nexus The DataNexus
     */
    protected QueryService(DataNexus nexus) {
        this.nexus = nexus;
    }

    /**
     * Return the DataNexus.
     *
     * @return The DataNexus
     */
    public DataNexus getNexus() {
        return nexus;
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
     * Query for PV extra info given PV metadata.
     *
     * @param metadata The PV metadata
     * @param type The type of info to query, null for all
     * @return The extra info
     * @throws SQLException If unable to query the database
     */
    public List<ExtraInfo> findExtraInfo(Metadata metadata, String type) throws SQLException {
        List<ExtraInfo> infoList = new ArrayList<>();

        String master = nexus.getMasterHostName();
        try (Connection con = nexus.getConnection(master)) {
            try (PreparedStatement stmt = nexus.getExtraInfoStatement(con, type)) {

                stmt.setInt(1, metadata.getId());

                if (type != null) {
                    stmt.setString(2, type);
                }

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String t = rs.getString("keyword");
                        Timestamp ts = rs.getTimestamp("stamp", Calendar.getInstance(TimeZone.getTimeZone("America/New_York")));
                        String value = rs.getString("info");

                        ExtraInfo info = new ExtraInfo(metadata, t, ts.toInstant(), value);
                        infoList.add(info);
                    }
                }
            }
        }

        return infoList;
    }
}
