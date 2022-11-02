package org.jlab.mya.nexus;

import java.io.IOException;
import java.nio.channels.Channel;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.commons.dbcp2.datasources.SharedPoolDataSource;
import org.mariadb.jdbc.MariaDbPoolDataSource;

/**
 * Allows use of the PooledNexus when outside of a Java application server such as Tomcat.
 *
 * JNDI DataSources are required for the PooledNexus to operate and generally these are configured
 * inside of an application server such as Tomcat. This class creates DataSources using the DBCP
 * pooling library to fill the role of an application server when there isn't one configured.
 *
 * @author slominskir
 */
public class StandaloneConnectionPools implements Channel {

    private final InitialContext initCtx;
    private final Context envCtx;
    private final List<SharedPoolDataSource> dsList = new ArrayList<>();

    /**
     * Creates DataSources for the deployment and publishes them to JNDI.
     *
     * @param deployment The MYA deployment
     * @throws NamingException If unable to lookup the connection pool
     * @throws SQLException If unable to query the database
     */
    public StandaloneConnectionPools(String deployment) throws NamingException, SQLException {
        initCtx = new InitialContext();
        envCtx = (Context) initCtx.lookup("java:comp/env");

        String user = OnDemandNexus.CREDENTIALS_PROPERTIES.getProperty("username");
        String password = OnDemandNexus.CREDENTIALS_PROPERTIES.getProperty("password");

        // Port is same for all hosts
        int port = Integer.parseInt(DataNexus.DEPLOYMENTS_PROPERTIES.getProperty("port"));

        String hostCsv = DataNexus.DEPLOYMENTS_PROPERTIES.getProperty(deployment + ".hosts");
        String[] hostList = hostCsv.split(",");

        for (String host : hostList) {

            String connectHost = host;
            int connectPort = port;

            String proxyHost = DataNexus.DEPLOYMENTS_PROPERTIES.getProperty("proxy.host." + host);
            String proxyPort = DataNexus.DEPLOYMENTS_PROPERTIES.getProperty("proxy.port." + host);

            if (proxyHost != null) {
                connectHost = proxyHost;
            }

            if (proxyPort != null) {
                connectPort = Integer.parseInt(proxyPort);
            }
            String url = "jdbc:mariadb://" + connectHost + ":" + connectPort + "/archive";

            MariaDbPoolDataSource pds = new MariaDbPoolDataSource();
            pds.setUrl(url);
            pds.setUser(user);
            pds.setPassword(password);

            /*pds.setUseCompression(true);
            pds.setNoAccessToProcedureBodies(true);

            pds.setCachePrepStmts(true);
            pds.setAllowPublicKeyRetrieval(true);
            pds.setSslMode("DISABLED");*/
            
            SharedPoolDataSource ds = new SharedPoolDataSource();
            ds.setConnectionPoolDataSource(pds);

            ds.setMaxTotal(20); // Max connections in the pool (regardless of idle vs active)
            
            envCtx.rebind("jdbc/" + host, ds);
            dsList.add(ds);
        }

    }

    @Override
    public boolean isOpen() {
        return !dsList.isEmpty();
    }

    @Override
    public void close() throws IOException {
        int errorCount = 0;
        for (SharedPoolDataSource ds : dsList) {
            try {
                ds.close();
            } catch (Exception e) {
                errorCount++;
            }
        }

        if (errorCount > 0) {
            throw new IOException("Unable to close");
        }
        
        dsList.clear();
    }
}
