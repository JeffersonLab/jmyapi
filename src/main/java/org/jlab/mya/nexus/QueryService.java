package org.jlab.mya.nexus;

import org.jlab.mya.*;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.IntEvent;
import org.jlab.mya.event.MultiStringEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
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
abstract class QueryService {

    /**
     * The DataNexus, which is the gateway to querying a Mya cluster.
     */
    protected final DataNexus nexus;
    protected final StatementGenerator generator = new StatementGenerator();

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
     * Query for PV metadata given PV name and specified type.
     *
     * @param name The PV name
     * @param type The type
     * @param <T> The Event type
     * @return PV metadata
     * @throws SQLException If unable to query the database
     * @throws ClassCastException If the PV has an incompatible type
     */
    @SuppressWarnings("unchecked")
    public <T extends Event> Metadata<T> findMetadata(String name, Class<T> type) throws SQLException, ClassCastException {
        return (Metadata<T>)findMetadata(name);
    }

    /**
     * Query for PV metadata given PV name.
     *
     * @param name The PV name
     * @return PV metadata
     * @throws SQLException If unable to query the database
     */
    @SuppressWarnings("unchecked")
    public Metadata findMetadata(String name) throws SQLException {
        Metadata metadata;

        String master = nexus.getMasterHostName();
        try (Connection con = nexus.getConnection(master)) {
            try (PreparedStatement stmt = generator.getMetadataStatement(con)) {

                stmt.setString(1, name);

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int id = rs.getInt("chan_id");
                        String host = rs.getString("host");
                        int typeOrdinal = rs.getInt("type");
                        int size = rs.getInt("size");

                        MyaDataType myaType = MyaDataType.values()[typeOrdinal];

                        Class javaType = getJavaType(size, myaType);

                        metadata = new Metadata(id, name, host, size, myaType, javaType);
                    } else {
                        metadata = null;
                    }
                }
            }
        }

        return metadata;
    }

    private Class getJavaType(int size, MyaDataType myaType) {
        Class type = null;

        if (size > 1) {
            type = MultiStringEvent.class;
        } else {
            switch (myaType) {
                case DBR_SHORT:
                case DBR_LONG:
                case DBR_ENUM:
                    type = IntEvent.class;
                    break;
                case DBR_FLOAT:
                case DBR_DOUBLE:
                    type = FloatEvent.class;
                    break;
                default:
                    type =  MultiStringEvent.class;
                    break;
            }
        }

        return type;
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
            try (PreparedStatement stmt = generator.getExtraInfoStatement(con, type)) {

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

    /**
     * Factory method for constructing an IntEvent from a row in a database
     * ResultSet.
     *
     * @param rs The ResultSet
     * @return A new IntEvent
     * @throws SQLException If unable to create an Event from the ResultSet
     */
    public static IntEvent intFromRow(ResultSet rs) throws SQLException {
        int codeOrdinal = rs.getInt(2);
        EventCode code = EventCode.fromInt(codeOrdinal);
        int value = rs.getInt(3);
        return new IntEvent(rs.getLong(1), code, value);
    }

    /**
     * Factory method for constructing a FloatEvent from a row in a database ResultSet.
     *
     * @param rs The ResultSet
     * @return A new FloatEvent
     * @throws SQLException If unable to create an Event from the ResultSet
     */
    public static FloatEvent floatFromRow(ResultSet rs) throws SQLException {
        int codeOrdinal = rs.getInt(2);
        EventCode code = EventCode.fromInt(codeOrdinal);
        float value = rs.getFloat(3);
        return new FloatEvent(rs.getLong(1), code, value);
    }

    /**
     * Factory method for constructing a MultiStringEvent from a row in a
     * database ResultSet.
     *
     * @param rs The ResultSet
     * @param size The size of the value vector (size 1 is acceptable)
     * @return A new MultiStringEvent
     * @throws SQLException If unable to create an Event from the ResultSet
     */
    public static MultiStringEvent fromRow(ResultSet rs, int size) throws SQLException {
        Instant timestamp = TimeUtil.fromMyaTimestamp(rs.getLong(1));
        int codeOrdinal = rs.getInt(2);
        EventCode code = EventCode.fromInt(codeOrdinal);
        String[] value = new String[size];
        int offset = 3;

        for (int i = 0; i < size; i++) {
            value[i] = rs.getString(i + offset);
        }
        return new MultiStringEvent(timestamp, code, value);
    }
}
