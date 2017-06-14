package org.jlab.mya;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jlab.mya.stream.FloatEventStream;
import org.jlab.mya.stream.IntEventStream;
import org.jlab.mya.stream.MultiStringEventStream;

/**
 *
 * @author ryans
 */
public class QueryService {

    private final DataNexus nexus;

    public QueryService(DataNexus nexus) {
        this.nexus = nexus;
    }

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

    public FloatEventStream openFloat(QueryParams params) throws SQLException {
        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);
        PreparedStatement stmt = nexus.getEventStatement(con, params);
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getBegin()));
        stmt.setLong(2, TimeUtil.toMyaTimestamp(params.getEnd()));
        ResultSet rs = stmt.executeQuery();
        return new FloatEventStream(params, con, stmt, rs);
    }

    public IntEventStream openInt(QueryParams params) throws SQLException {
        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);
        PreparedStatement stmt = nexus.getEventStatement(con, params);
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getBegin()));
        stmt.setLong(2, TimeUtil.toMyaTimestamp(params.getEnd()));
        ResultSet rs = stmt.executeQuery();
        return new IntEventStream(params, con, stmt, rs);
    }

    public MultiStringEventStream openMultiString(QueryParams params) throws SQLException {
        String host = params.getMetadata().getHost();
        Connection con = nexus.getConnection(host);
        PreparedStatement stmt = nexus.getEventStatement(con, params);
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getBegin()));
        stmt.setLong(2, TimeUtil.toMyaTimestamp(params.getEnd()));
        ResultSet rs = stmt.executeQuery();
        return new MultiStringEventStream(params, con, stmt, rs);
    }

    public DataNexus getNexus() {
        return nexus;
    }
}
