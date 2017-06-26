package org.jlab.mya;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Provides query access to the Mya database.
 *
 * This class is abstract because it simply sets up the framework for specialized services to do the
 * heavy lifting. This base class houses the DataNexus and Metadata lookup, both of which are
 * required for any specialized query service to function.
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
    public QueryService(DataNexus nexus) {
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
}
