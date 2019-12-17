package org.jlab.mya.nexus;

import org.jlab.mya.*;
import org.jlab.mya.event.*;

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
     * Paginated wildcard query for a list of PVs (Channels) like the specified name.  Standard SQL wildcards (% and _)
     * are supported as an SQL like query is performed.   For example to query for a name containing "R123" you
     * would use "%R123%".
     *
     * @param q The wildcard search query
     * @param limit max results
     * @param offset page though query starting index (start at zero)
     * @return The paginated list of channel metadata like name
     * @throws SQLException If unable to query the database
     */
    @SuppressWarnings("unchecked")
    public List<Metadata> findChannel(String q, long limit, long offset) throws SQLException {
        List<Metadata> metadataList = new ArrayList<>();

        String master = nexus.getMasterHostName();
        try (Connection con = nexus.getConnection(master)) {
            try (PreparedStatement stmt = generator.getChannelStatement(con)) {

                stmt.setString(1, q);
                stmt.setLong(2, limit);
                stmt.setLong(3, offset);

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        Metadata metadata = unmarshallMetadata(rs);
                        metadataList.add(metadata);
                    }
                }
            }
        }

        return metadataList;
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
                        metadata = unmarshallMetadata(rs);
                    } else {
                        metadata = null;
                    }
                }
            }
        }

        return metadata;
    }

    @SuppressWarnings("unchecked")
    private Metadata unmarshallMetadata(ResultSet rs) throws SQLException {
        int id = rs.getInt("chan_id");
        String name = rs.getString("name");
        String host = rs.getString("host");
        String ioc = rs.getString("ioc");
        boolean active = rs.getShort("active") == 1;
        int typeOrdinal = rs.getInt("type");
        int size = rs.getInt("size");

        MyaDataType myaType = MyaDataType.values()[typeOrdinal];

        Class javaType = getJavaType(size, myaType);

        return new Metadata(id, name, host, size, ioc, active, myaType, javaType);
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
     * @param begin limit results to just the first one before begin plus all afterwards or null for all
     * @param end limit results to just those before end or null for all
     * @return The extra info
     * @throws SQLException If unable to query the database
     */
    public List<ExtraInfo> findExtraInfo(Metadata metadata, String type, Instant begin, Instant end) throws SQLException {
        List<ExtraInfo> infoList = new ArrayList<>();

        String master = nexus.getMasterHostName();
        try (Connection con = nexus.getConnection(master)) {
            try (PreparedStatement stmt = generator.getExtraInfoStatement(con, type, end)) {

                stmt.setInt(1, metadata.getId());

                if (type != null) {
                    stmt.setString(2, type);
                }

                if(end != null) {
                    stmt.setTimestamp(3, Timestamp.from(end));
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

        // We filter out start bounds here instead of querying database twice (we need prior point!)
        if(begin != null) {
            int fromIndex = 0;

            for(int i = 0; i < infoList.size(); i++) {
                ExtraInfo info = infoList.get(i);
                //System.out.println("info: " + info);
                if(info.getTimestamp().isBefore(begin)) {
                    //System.out.println("found new prior point: " + i);
                    fromIndex = i;
                } else {
                    break;
                }
            }

            if(fromIndex > 0) {
                int toIndex = infoList.size(); // exclusive, apparently :)
                infoList = infoList.subList(fromIndex, toIndex);
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
