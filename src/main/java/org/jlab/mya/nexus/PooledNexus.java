package org.jlab.mya.nexus;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 * A Mya DataNexus which pools resources for later use.
 *
 * <p>This DataNexus delegates the connection pooling to one or more JNDI DataSources. Either a
 * container (application server) such as Tomcat needs to be configured with the JNDI DataSources or
 * the StandaloneJndi and StandaloneConnectionPools classes can be used.
 *
 * @author slominskir
 */
public class PooledNexus extends DataNexus {

  private final Context initCtx;
  private final Context envCtx;
  private final Map<String, DataSource> dsMap = new HashMap<>();

  /**
   * Create a new PooledNexus with the specified deployment.
   *
   * @param deployment The deployment
   * @throws NamingException If unable to lookup the underlying DBCP connection pool
   */
  public PooledNexus(String deployment) throws NamingException {
    super(deployment);

    initCtx = new InitialContext();
    envCtx = (Context) initCtx.lookup("java:comp/env");

    String hostCsv = DEPLOYMENTS_PROPERTIES.getProperty(deployment + ".hosts");
    String[] hostList = hostCsv.split(",");

    for (String host : hostList) {
      DataSource ds;
      try {
        ds = (DataSource) envCtx.lookup("jdbc/" + host);
      } catch (NamingException e) {
        // The NamingException thrown by Tomcat doesn't provide any info such as which
        // name failed so we use our own
        throw new NamingException("Unable to find DataSource: jdbc/" + host);
      }
      dsMap.put(host, ds);
    }
  }

  @Override
  Connection getConnection(String host) throws SQLException {
    DataSource ds = dsMap.get(host);

    if (ds == null) {
      throw new SQLException("Unable to obtain DataSource for: " + host);
    }

    try {
      return ds.getConnection();
    } catch (SQLException e) {
      throw new SQLException(
          "Unable to obtain Connection for: "
              + host
              + "\nSQL State: "
              + e.getSQLState()
              + "\nSQL Exception Message: "
              + e.getMessage()
              + "\nError Code: "
              + e.getErrorCode(),
          e);
    }
  }
}
