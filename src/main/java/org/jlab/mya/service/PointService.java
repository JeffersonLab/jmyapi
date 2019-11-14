package org.jlab.mya.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.jlab.mya.*;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.IntEvent;
import org.jlab.mya.event.MultiStringEvent;
import org.jlab.mya.params.PointQueryParams;
import org.jlab.mya.DataNexus;
import org.jlab.mya.Event;
import org.jlab.mya.QueryService;
import org.jlab.mya.TimeUtil;

/**
 * Provides query access to the Mya database for a single event near a point in
 * time.
 *
 * @author slominskir
 */
public class PointService extends QueryService {

    /**
     * Create a new PointService with the provided DataNexus.
     *
     * @param nexus The DataNexus
     */
    public PointService(DataNexus nexus) {
        super(nexus);
    }

    public Event findEvent(PointQueryParams params) throws SQLException {

        Event event;

        if (params.getMetadata().getSize() > 1) {
            event = findMultiStringEvent(params);
        } else {
            switch (params.getMetadata().getType()) {
                case DBR_SHORT:
                case DBR_LONG:
                case DBR_ENUM:
                    event = findIntEvent(params);
                    break;
                case DBR_FLOAT:
                case DBR_DOUBLE:
                    event = findFloatEvent(params);
                    break;
                default:
                    event = findMultiStringEvent(params);
                    break;
            }
        }

        return event;
    }

    /**
     * Find a float-valued event associated with the specified PointQueryParams.
     *
     * @param params The PointQueryParams
     * @return A FloatEvent or null if none found
     * @throws SQLException If unable to query the database
     */
    public FloatEvent findFloatEvent(PointQueryParams params) throws SQLException {
        String host = params.getMetadata().getHost();
        try (Connection con = nexus.getConnection(host)) {
            try (PreparedStatement stmt = nexus.getEventPointStatement(con, params)) {
                stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getTimestamp()));
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return FloatEvent.fromRow(rs);
                    } else {
                        return null;
                    }
                }
            }
        }
    }

    /**
     * Find an int-valued event associated with the specified PointQueryParams.
     *
     * @param params The PointQueryParams
     * @return An IntEvent or null if none found
     * @throws SQLException If unable to query the database
     */
    public IntEvent findIntEvent(PointQueryParams params) throws SQLException {
        String host = params.getMetadata().getHost();
        try (Connection con = nexus.getConnection(host)) {
            try (PreparedStatement stmt = nexus.getEventPointStatement(con, params)) {
                stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getTimestamp()));
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return IntEvent.fromRow(rs);
                    } else {
                        return null;
                    }
                }
            }
        }
    }

    /**
     * Find a multi-string-valued event associated with the specified
     * PointQueryParams.
     *
     * @param params The PointQueryParams
     * @return A MultiStringEvent or null if none found
     * @throws SQLException If unable to query the database
     */
    public MultiStringEvent findMultiStringEvent(PointQueryParams params) throws SQLException {
        String host = params.getMetadata().getHost();
        try (Connection con = nexus.getConnection(host)) {
            try (PreparedStatement stmt = nexus.getEventPointStatement(con, params)) {
                stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getTimestamp()));
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return MultiStringEvent.fromRow(rs, params.getMetadata().getSize());
                    } else {
                        return null;
                    }
                }
            }
        }
    }
}