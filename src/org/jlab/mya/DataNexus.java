package org.jlab.mya;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Manages the possibly multiple datasources required to service requests to a cluster of Mya hosts.
 * 
 * @author ryans
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
        // Load MySQL Driver
        // TODO: I don't think this is needed anymore
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }

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
    
    public DataNexus(Deployment deployment) {
        this.deployment = deployment;
    }

    public Deployment getDeployment() {
        return deployment;
    }
    
    public String getMasterHostName() {
        return DEPLOYMENTS_PROPERTIES.getProperty(deployment + ".master.host");
    }
    
    public abstract Connection getConnection(String host) throws SQLException;
    
    public abstract PreparedStatement getMetadataStatement(Connection con) throws SQLException;
    
    public abstract PreparedStatement getEventStatement(Connection con, QueryParams params) throws SQLException;
    
    public abstract PreparedStatement getCountStatement(Connection con, QueryParams params) throws SQLException;    
}
