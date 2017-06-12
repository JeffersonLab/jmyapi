package org.jlab.mya.jmyapi;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

/**
 *
 * @author ryans
 */
public class MyGet {

    public PvMetadata fetchMetadata(Connection con, String name) throws SQLException {

        try (PreparedStatement stmt = createMetadataStatement(con)) {
            return fetchMetadata(stmt, name);
        }
    }

    public PreparedStatement createMetadataStatement(Connection con) throws SQLException {
        String query = "select * from channels where name = ?";
        return con.prepareStatement(query);
    }

    public PvMetadata fetchMetadata(PreparedStatement stmt, String name) throws SQLException {
        PvMetadata metadata;

        stmt.setString(1, name);

        try (ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                int id = rs.getInt("chan_id");
                String host = rs.getString("host");

                metadata = new PvMetadata(id, name, host);
            } else {
                metadata = null;
            }
        }

        return metadata;
    }

    public long countRecords(Connection con, int id, Instant begin, Instant end) throws SQLException {
        String query = "select count(*) from table_" + id + " where time >= ? and time < ?";
        long count;

        try (PreparedStatement stmt = con.prepareStatement(query)) {
            stmt.setLong(1, MyaUtil.toMyaTimestamp(begin));
            stmt.setLong(2, MyaUtil.toMyaTimestamp(end));
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    count = rs.getLong(1);
                } else {
                    throw new SQLException("Unable to count records in table_" + id);
                }
            }
        }

        return count;
    }

    public List<PvRecord> fetchList(Connection con, int id, Instant begin, Instant end) throws
            SQLException, IOException {
        List<PvRecord> recordList = new ArrayList<>();

        try (PvRecordStream s = openStream(con, id, begin, end)) {
            PvRecord record;

            while((record = s.read()) != null) {
                recordList.add(record);                
            }
        }

        return recordList;
    }

    public PvRecordStream openStream(Connection con, int id, Instant begin, Instant end) throws
            SQLException { // TODO: is end exclusive or inclusive?
        String query = "select * from table_" + id
                + " where time >= ? and time < ? order by time desc";
        // TODO: prepareStatement might not allow streaming so might have to use createStatement
        PreparedStatement stmt = con.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE); // MySQL Driver specific hint
        stmt.setLong(1, MyaUtil.toMyaTimestamp(begin));
        stmt.setLong(2, MyaUtil.toMyaTimestamp(end));
        ResultSet rs = stmt.executeQuery();
        return new PvRecordStream(stmt, rs);
    }
}
