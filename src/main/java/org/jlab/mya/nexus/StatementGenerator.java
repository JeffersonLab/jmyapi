package org.jlab.mya.nexus;

import org.jlab.mya.event.EventCode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Responsible for generating SQL statements.
 *
 * @author slominskir
 */
class StatementGenerator {
    /**
     * Get the SQL list of event code numbers that match to data updates.  E.g., "where column in [some_list]".
     * @return A string suitable for use in a SQL query.
     */
    private static String getDataEventListString() {
        return "(" + String.join(",", EventCode.getDataEventCodes().stream()
                .map((EventCode e)->String.valueOf(e.getCodeNumber()))
                .collect(Collectors.toSet()))  + ")";
    }
    /**
     * Return a prepared statement for the given connection to query for channel names.
     *
     * Note: clever implementations may be caching / pooling statements.
     *
     * @param con The connection the statement belongs to
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    PreparedStatement getChannelStatement(Connection con) throws SQLException {
        String query = "select * from channels where name like ? order by name asc limit ? offset ?";
        return con.prepareStatement(query);
    }

    /**
     * Return a prepared statement for the given connection to query metadata.
     *
     * Note: clever implementations may be caching / pooling statements.
     *
     * @param con The connection the statement belongs to
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    PreparedStatement getMetadataStatement(Connection con) throws SQLException {
        String query = "select * from channels where name = ?";
        return con.prepareStatement(query);
    }

    /**
     * Return a prepared statement for the given connection to query extra info.
     *
     * Note: clever implementations may be caching / pooling statements.
     *
     * @param con The connection the statement belongs to
     * @param type The type of info to query, null for all
     * @param end limit results to just those before end or null for all
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    PreparedStatement getExtraInfoStatement(Connection con, String type, Instant end) throws SQLException {
        String query = "select * from metadata where chan_id = ? ";

        if (type != null) {
            query = query + "and keyword = ? ";
        }

        if(end != null) {
            query = query + "and stamp < ? ";
        }

        query = query + "order by stamp asc";
        return con.prepareStatement(query);
    }

    /**
     * Return a prepared statement for the given connection and parameters to
     * query a time interval for events.
     * <p>
     * The fetchSize parameter should be used carefully.  Use value Integer.MIN_VALUE to instruct MySQL driver to
     * stream results.  This is the safest in terms of ensuring you don't run out of memory, but may not be the
     * fastest method. If set to a positive number 'n' AND com.mysql.jdbc.Connection.setUseCursorFetch(true) then a
     * server-side cursor (temporary table) is used to spoon feed the results 'n' at a time.  This is likely the worst
     * option as results don't start arriving on the client until after the server has fully filled the temporary table.
     * This option puts more stress on the server too. Finally, the third option is enabled if fetchSize is
     * not Integer.MIN_VALUE and setUserCursorFetch(true) is also not set.  In this case the fetchSize value is ignored
     * (as if set to zero) and the MySQL driver will fetch the entire ResultSet all at once into memory.
     * This may be the fastest method (for small data), but may cause your JVM to run  out of memory (with large data).
     * </p>
     *
     * @param con The connection the statement belongs to
     * @param params The query parameters associated with the statement (notably
     * metadata id)
     * @param fetchSize MySQL Database Driver fetch size option
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    PreparedStatement getEventIntervalStatement(Connection con, IntervalQueryParams params, int fetchSize) throws
            SQLException {

        String where = getIntervalWhereClause(params);


        String query = "select * from table_" + params.getMetadata().getId();

        query = query + where;

        query = query + " order by time asc";

        PreparedStatement stmt = con.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        return stmt;
    }

    /**
     * Get shared where clause for interval and count queries.
     *
     * @param params The parameters
     * @return The SQL where clause
     */
    private String getIntervalWhereClause(IntervalQueryParams params) {
        List<String> filterList = new ArrayList<>();

        filterList.add("time >= ?");
        filterList.add("time < ?");

        if (params.isUpdatesOnly()) {
            filterList.add("code in " + getDataEventListString());
        }

        String where = "";

        if (!filterList.isEmpty()) {
            where = " where " + filterList.get(0);

            for (int i = 1; i < filterList.size(); i++) {
                where = where + " and " + filterList.get(i);
            }
        }

        return where;
    }

    /**
     * Return a prepared statement for the given connection and parameters to
     * count events in a time interval.
     *
     * @param con The connection the statement belongs to
     * @param params The query parameters associated with the statement (notably
     * metadata id)
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    PreparedStatement getCountStatement(Connection con, IntervalQueryParams params) throws
            SQLException {

        String where = getIntervalWhereClause(params);

        String query = "select count(*) from table_" + params.getMetadata().getId();

        query = query + where;

        return con.prepareStatement(query);
    }

    /**
     * Return a prepared statement for the given connection and parameters to
     * query for a single event at a given point in time. Depending on
     * parameters the query may search for the last event before (or equal) the
     * point-in-time or the first event after (or equal) the point in time.
     *
     * @param con The connection the statement belongs to
     * @param params The query parameters associated with the statement (notably
     * metadata id)
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    PreparedStatement getEventPointStatement(Connection con, PointQueryParams params) throws
            SQLException {
        String equal = "";
        String sign = ">";
        String sort = "asc";

        if (params.isOrEqual()) {
            equal = "=";
        }

        if (params.isLessThan()) {
            sign = "<";
            sort = "desc";
        }

        List<String> filterList = new ArrayList<>();

        filterList.add("time " + sign + equal + " ?");

        if (params.isUpdatesOnly()) {
            filterList.add("code in " + getDataEventListString());
        }

        String where = "";

        if (!filterList.isEmpty()) {
            where = " where " + filterList.get(0);

            for (int i = 1; i < filterList.size(); i++) {
                where = where + " and " + filterList.get(i);
            }
        }

        String query = "select * from table_" + params.getMetadata().getId() + " force index for order by (primary)";

        query = query + where;

        query = query + " order by time " + sort + " limit 1";

        //System.out.println(query);

        return con.prepareStatement(query);
    }
}
