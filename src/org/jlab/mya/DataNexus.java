package org.jlab.mya;

import org.jlab.mya.params.IntervalQueryParams;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import org.jlab.mya.params.PointQueryParams;

/**
 * Manages the possibly multiple data sources required to service requests to a cluster of Mya
 * hosts.
 *
 * Two configuration files (Java properties) are required to be in the class path: (1)
 * credentials.properties and (2) deployments.properties. If these files are not found this class
 * will throw an ExceptionInInitializerError upon being loaded by the class loader.
 *
 * @author slominskir
 */
public abstract class DataNexus {

    /**
     * The application's db credentials configuration properties.
     */
    public static final Properties CREDENTIALS_PROPERTIES = new Properties();

    /**
     * The application's deployments configuration properties.
     */
    public static final Properties DEPLOYMENTS_PROPERTIES = new Properties();

    static {
        // Load DB Credentials Config
        try (InputStream is
                = DataNexus.class.getClassLoader().getResourceAsStream(
                        "credentials.properties")) {
            if (is == null) {
                throw new IOException(
                        "File Not Found; Configuration File: credentials.properties");
            }

            CREDENTIALS_PROPERTIES.load(is);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }

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
     * Return the host name of the master host for the deployment associated with this DataNexus.
     *
     * @return The master host name
     */
    public String getMasterHostName() {
        return DEPLOYMENTS_PROPERTIES.getProperty(deployment + ".master.host");
    }

    /**
     * Return a connection to the specified host.
     *
     * Note: clever implementations may be caching / pooling connections and may actually return a
     * wrapped connection to you.
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
    public abstract PreparedStatement getMetadataStatement(Connection con) throws SQLException;

    /**
     * Return a prepared statement for the given connection and query parameters to query events.
     *
     * @param con The connection the statement belongs to
     * @param params The query parameters associated with the statement (notably metadata id)
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    public abstract PreparedStatement getEventIntervalStatement(Connection con,
            IntervalQueryParams params) throws
            SQLException;

    /**
     * Return a prepared statement for the given connection and query parameters to count events.
     *
     * @param con The connection the statement belongs to
     * @param params The query parameters associated with the statement (notably metadata id)
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    public abstract PreparedStatement getCountStatement(Connection con, IntervalQueryParams params) throws
            SQLException;

    /**
     * Return a prepared statement for obtaining a single event at a given point in time (first
     * event; greater than or equal to timestamp).
     *
     * @param con The connection the statement belongs to
     * @param params The query parameters associated with the statement (notably metadata id)
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    public abstract PreparedStatement getEventPointFirstStatement(Connection con,
            PointQueryParams params) throws SQLException;

    /**
     * Return a prepared statement for obtaining a single event at a given point in time (last
     * event; less than or equal to timestamp).
     *
     * @param con The connection the statement belongs to
     * @param params The query parameters associated with the statement (notably metadata id)
     * @return The PreparedStatement
     * @throws SQLException If unable to prepare a statement
     */
    public abstract PreparedStatement getEventPointLastStatement(Connection con,
            PointQueryParams params) throws SQLException;
}
