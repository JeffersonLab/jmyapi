package org.jlab.mya;

import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.params.PointQueryParams;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Manages the possibly multiple data sources required to service requests to a
 * cluster of Mya hosts, which make up a deployment.
 *
 * A configuration file (Java properties) is required to be in the class path:
 * "deployments.properties". If this file is not found this class will throw an
 * ExceptionInInitializerError upon being loaded by the class loader.
 *
 * @author slominskir
 */
@SuppressWarnings("DuplicatedCode")
public abstract class DataNexus {

    /**
     * The application's deployments configuration properties.
     */
    public static final Properties DEPLOYMENTS_PROPERTIES = new Properties();

    static {
        // Load MYA Deployment Config
        try (InputStream is
                = DataNexus.class.getClassLoader().getResourceAsStream(
                "deployments.properties")) {
            if (is == null) {
                throw new IOException(
                        "File Not Found; Configuration File: deployments.properties");
            }

            DEPLOYMENTS_PROPERTIES.load(is);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final String deployment;

    /**
     * Create a new DataNexus for the given deployment.
     *
     * @param deployment The Mya deployment
     */
    public DataNexus(String deployment) {
        if (DataNexus.getDeploymentNames().contains(deployment)) {
            this.deployment = deployment;
        } else {
            throw new IllegalArgumentException("Deployment definition not found: " + deployment);
        }
    }

    /**
     * Return the Mya deployment.
     *
     * @return The deployment
     */
    public String getDeployment() {
        return deployment;
    }
    
    /**
     * Return the set of valid deployment names.
     * @return The Set of valid deployment names.
     */
    public static Set<String> getDeploymentNames() {
        Set<String> names = new TreeSet<>();
        for(Object o : DEPLOYMENTS_PROPERTIES.keySet()) {
            String prop = (String) o;
            if (prop.matches("\\w+.master.host")) {
                names.add(prop.split("\\.")[0]);
            }
        }
        return names;
    }
    
    /**
     * Return the host name of the master host for the deployment associated
     * with this DataNexus.
     *
     * @return The master host name
     */
    public String getMasterHostName() {
        return DEPLOYMENTS_PROPERTIES.getProperty(deployment + ".master.host");
    }

    /**
     * Return a connection to the specified host.
     *
     * Note: clever implementations may be caching / pooling connections and may
     * actually return a wrapped connection to you.
     *
     * @param host The Mya host name
     * @return A MySQL database connection (or wrapped connection)
     * @throws SQLException If unable to obtain a connection
     */
    public abstract Connection getConnection(String host) throws SQLException;

    /**
     * Return a prepared statement for the given connection to query metadata.
     *
     * Note: clever implementations may be caching / pooling statements.
     *
     * @param con The connection the statement belongs to
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    public PreparedStatement getMetadataStatement(Connection con) throws SQLException {
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
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    public PreparedStatement getExtraInfoStatement(Connection con, String type) throws SQLException {
        String query = "select * from metadata where chan_id = ? ";

        if (type != null) {
            query = query + "and keyword = ? ";
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
    public PreparedStatement getEventIntervalStatement(Connection con, IntervalQueryParams params, int fetchSize) throws
            SQLException {

        String where = getIntervalWhereClause(params);


        String query = "select * from table_" + params.getMetadata().getId();

        query = query + where;

        query = query + " order by time asc";

        PreparedStatement stmt = con.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize); // MySQL Driver specific hint that should be used wisely
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
            filterList.add("code = " + EventCode.UPDATE.getCodeNumber());
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
    public PreparedStatement getCountStatement(Connection con, IntervalQueryParams params) throws
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
    public PreparedStatement getEventPointStatement(Connection con, PointQueryParams params) throws
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
            filterList.add("code = " + EventCode.UPDATE.getCodeNumber());
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
