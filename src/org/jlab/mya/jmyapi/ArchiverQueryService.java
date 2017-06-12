package org.jlab.mya.jmyapi;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * Note: The first time you reference the ArchiverQueryService the JVM may generate an
 * ExceptionInInitializerError if the JVM class loader cannot find the configuration files.
 *
 * @author ryans
 */
public class ArchiverQueryService {

    private static final Logger LOGGER = Logger.getLogger(ArchiverQueryService.class.getName());

    /**
     * The application's db credentials configuration properties.
     */
    public static final Properties CREDENTIALS_PROPERTIES = new Properties();

    /**
     * The application's deployments configuration properties.
     */
    public static final Properties DEPLOYMENTS_PROPERTIES = new Properties();

    static {
        // Load MySQL Driver
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }

        // Load DB Credentials Config
        try (InputStream is
                = ArchiverQueryService.class.getClassLoader().getResourceAsStream(
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
                = ArchiverQueryService.class.getClassLoader().getResourceAsStream(
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

    private final ArchiverDeployment deployment;

    public ArchiverQueryService(ArchiverDeployment deployment) {
        this.deployment = deployment;

        if (deployment == null) {
            throw new NullPointerException("Deployment must not be null");
        }
    }

    public Connection open(String host, int port) throws SQLException {
        String user = CREDENTIALS_PROPERTIES.getProperty("username");
        String password = CREDENTIALS_PROPERTIES.getProperty("password");

        String proxyHost = DEPLOYMENTS_PROPERTIES.getProperty("proxy.host." + host);
        String proxyPort = DEPLOYMENTS_PROPERTIES.getProperty("proxy.port." + host);

        if (proxyHost != null) {
            host = proxyHost;
        }

        if (proxyPort != null) {
            port = Integer.parseInt(proxyPort);
        }

        String url = "jdbc:mysql://" + host + ":" + port + "/archive";

        return DriverManager.getConnection(url, user, password);
    }

    public List<PvRecord> find(String name, Instant begin, Instant end) throws SQLException,
            IOException {
        List<PvRecord> recordList;

        String host = DEPLOYMENTS_PROPERTIES.getProperty(deployment + ".master.host");
        int port = Integer.parseInt(DEPLOYMENTS_PROPERTIES.getProperty("port"));

        try (Connection con = open(host, port)) {
            MyGet get = new MyGet();
            PvMetadata metadata = get.fetchMetadata(con, name);
            if (host.equals(metadata.getHost())) { // TODO: check for better equivalence: fully qualified vs not vs loopback, etc.
                recordList = get.fetchList(con, metadata.getId(), begin, end);
            } else {
                recordList = find(metadata, begin, end);
            }
        }

        return recordList;
    }

    public List<PvRecord> find(PvMetadata metadata, Instant begin, Instant end) throws SQLException,
            IOException {
        List<PvRecord> recordList;
        String host = metadata.getHost();
        int port = Integer.parseInt(DEPLOYMENTS_PROPERTIES.getProperty("port"));

        try (Connection con = open(host, port)) {
            MyGet get = new MyGet();
            recordList = get.fetchList(con, port, begin, end);
        }

        return recordList;
    }
}
