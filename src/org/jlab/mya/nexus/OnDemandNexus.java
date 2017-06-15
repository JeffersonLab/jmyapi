package org.jlab.mya.nexus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.jlab.mya.DataNexus;
import org.jlab.mya.Deployment;
import org.jlab.mya.QueryParams;

/**
 *
 * @author slominskir
 */
public class OnDemandNexus extends DataNexus {

    public OnDemandNexus(Deployment deployment) {
        super(deployment);
    }

    @Override
    public Connection getConnection(String host) throws SQLException {
        String user = CREDENTIALS_PROPERTIES.getProperty("username");
        String password = CREDENTIALS_PROPERTIES.getProperty("password");

        // Port is same for all hosts
        int port = Integer.parseInt(DEPLOYMENTS_PROPERTIES.getProperty("port"));

        String proxyHost = DEPLOYMENTS_PROPERTIES.getProperty("proxy.host." + host);
        String proxyPort = DEPLOYMENTS_PROPERTIES.getProperty("proxy.port." + host);

        if (proxyHost != null) {
            host = proxyHost;
        }

        if (proxyPort != null) {
            port = Integer.parseInt(proxyPort);
        }

        String url = "jdbc:mysql://" + host + ":" + port + "/archive";

        Properties options = new Properties();
        
        options.put("user", user);
        options.put("password", password);
        
        options.put("useCompression", "true"); // 1.5 Million records streams in 8 seconds with compression and 16 seconds without
        
        return DriverManager.getConnection(url, options);
    }

    @Override
    public PreparedStatement getMetadataStatement(Connection con) throws SQLException {
        String query = "select * from channels where name = ?";
        return con.prepareStatement(query);
    }

    @Override
    public PreparedStatement getEventStatement(Connection con, QueryParams params) throws
            SQLException {
        String query = "select * from table_" + params.getMetadata().getId()
                + " where time >= ? and time < ? order by time asc";
        PreparedStatement stmt = con.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE); // MySQL Driver specific hint
        return stmt;
    }

    @Override
    public PreparedStatement getCountStatement(Connection con, QueryParams params) throws SQLException {
        String query = "select count(*) from table_" + params.getMetadata().getId() + " where time >= ? and time < ?";

        return con.prepareStatement(query);
    }

}
