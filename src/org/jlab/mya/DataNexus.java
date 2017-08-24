package org.jlab.mya;

import org.jlab.mya.params.IntervalQueryParams;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.jlab.mya.params.PointQueryParams;

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

    private final Deployment deployment;

    /**
     * Create a new DataNexus for the given deployment.
     *
     * @param deployment The Mya deployment
     */
    public DataNexus(Deployment deployment) {
        this.deployment = deployment;
    }

    /**
     * Return the Mya deployment.
     *
     * @return The deployment
     */
    public Deployment getDeployment() {
        return deployment;
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
     *
     * @param con The connection the statement belongs to
     * @param params The query parameters associated with the statement (notably
     * metadata id)
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    public PreparedStatement getEventIntervalStatement(Connection con, IntervalQueryParams params) throws
            SQLException {
        String query = "select * from table_" + params.getMetadata().getId()
                + " where time >= ? and time < ? order by time asc";
        PreparedStatement stmt = con.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE); // MySQL Driver specific hint
        return stmt;
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
        String query = "select count(*) from table_" + params.getMetadata().getId()
                + " where time >= ? and time < ?";

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

        String query = "select * from table_" + params.getMetadata().getId()
                + " where time " + sign + equal + " ? order by time " + sort + " limit 1";

        return con.prepareStatement(query);
    }
}
