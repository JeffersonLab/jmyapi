package org.jlab.mya.nexus;

import java.io.IOException;
import java.nio.channels.Channel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.jlab.mya.DataNexus;
import org.jlab.mya.Deployment;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.params.PointQueryParams;

/**
 * A Mya DataNexus which pools resources for later use (Not supported yet). 
 * 
 * @author slominskir
 */
public class PooledNexus extends DataNexus implements Channel {

    /**
     * Create a new PooledNexus with the specified deployment.
     * 
     * @param deployment The deployment
     */    
    public PooledNexus(Deployment deployment) {
        super(deployment);
    }

    @Override
    public boolean isOpen() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Connection getConnection(String host) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement getMetadataStatement(Connection con) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement getEventIntervalStatement(Connection con, IntervalQueryParams params) throws
            SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement getCountStatement(Connection con, IntervalQueryParams params) throws
            SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement getEventPointStatement(Connection con, PointQueryParams params) throws
            SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
