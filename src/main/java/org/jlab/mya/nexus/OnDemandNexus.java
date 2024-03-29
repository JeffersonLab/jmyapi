package org.jlab.mya.nexus;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A Mya DataNexus which creates resources on-demand and closes them immediately after use.
 *
 * A configuration file (Java properties) is required to be in the class path:
 * "credentials.properties". If this file is not found this class will throw an
 * ExceptionInInitializerError upon being loaded by the class loader.
 *
 * @author slominskir
 */
public class OnDemandNexus extends DataNexus {

    /**
     * The application's db credentials configuration properties.
     */
    public static final Properties CREDENTIALS_PROPERTIES = new Properties();

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
    }

    /**
     * Create a new OnDemandNexus with the specified deployment.
     *
     * @param deployment The deployment
     */
    public OnDemandNexus(String deployment) {
        super(deployment);
    }

    @Override
    Connection getConnection(String host) throws SQLException {
        String user = CREDENTIALS_PROPERTIES.getProperty("username");
        String password = CREDENTIALS_PROPERTIES.getProperty("password");

        // Port is same for all hosts
        int port = Integer.parseInt(DEPLOYMENTS_PROPERTIES.getProperty("port"));

        if("true".equals(System.getenv("JMYAPI_USE_PROXY"))) {
            String proxyHost = DEPLOYMENTS_PROPERTIES.getProperty("proxy.host." + host);
            String proxyPort = DEPLOYMENTS_PROPERTIES.getProperty("proxy.port." + host);

            if (proxyHost != null) {
                host = proxyHost;
            }

            if (proxyPort != null) {
                port = Integer.parseInt(proxyPort);
            }
        }

        String url = "jdbc:mariadb://" + host + ":" + port + "/archive";

        Properties options = new Properties();

        options.put("user", user);
        options.put("password", password);

        options.put("useCompression", "true"); // 1.5 Million records streams in 8 seconds with compression and 16 seconds without

        options.put("noAccessToProcedureBodies", "true"); // Required to use stored procedures from limited access user

        options.put("sslMode", "DISABLED");
        options.put("allowPublicKeyRetrieval", "true");

        try {
            return DriverManager.getConnection(url, options);
        } catch (SQLException e) {
            throw new SQLException("Unable to obtain connection to host: " + host + "; port: "
                    + port, e);
        }
    }
}
