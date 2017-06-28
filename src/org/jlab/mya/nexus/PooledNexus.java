package org.jlab.mya.nexus;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.jlab.mya.Deployment;

;

/**
 * A Mya DataNexus which pools resources for later use.
 *
 * This DataNexus delegates the connection pooling to one or more JNDI DataSources. Either a
 * container (application server) such as Tomcat needs to be configured with the JNDI DataSources or
 * the StandaloneJndi and StandaloneConnectionPools classes can be used.
 * 
 * @author slominskir
 */
public class PooledNexus extends OnDemandNexus {

    private final Context initCtx;
    private final Context envCtx;
    private final Map<String, DataSource> dsMap = new HashMap<>();

    /**
     * Create a new PooledNexus with the specified deployment.
     *
     * @param deployment The deployment
     * @throws NamingException If unable to lookup the underlying DBCP connection pool
     */
    public PooledNexus(Deployment deployment) throws NamingException {
        super(deployment);

        initCtx = new InitialContext();
        envCtx = (Context) initCtx.lookup("java:comp/env");

        String hostCsv = DEPLOYMENTS_PROPERTIES.getProperty(deployment + ".hosts");
        String hostList[] = hostCsv.split(",");

        for (String host : hostList) {
            DataSource ds = (DataSource) envCtx.lookup("jdbc/" + host);
            dsMap.put(host, ds);
        }
    }

    @Override
    public Connection getConnection(String host) throws SQLException {
        DataSource ds = dsMap.get(host);

        if (ds == null) {
            throw new SQLException("Unable to obtain connection for: " + host);
        }

        return ds.getConnection();
    }
}
