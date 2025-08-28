package org.jlab.mya.nexus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.Event;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.IntEvent;
import org.jlab.mya.event.MultiStringEvent;

/**
 * Provides query access to the Mya database for a single event near a point in time.
 *
 * @author slominskir
 */
class PointService extends QueryService {

  /**
   * Create a new PointService with the provided DataNexus.
   *
   * @param nexus The DataNexus
   */
  public PointService(DataNexus nexus) {
    super(nexus);
  }

  /**
   * Find an event associated with the specified PointQueryParams.
   *
   * @param params The PointQueryParams
   * @param <T> The event type
   * @return An Event or null if none found
   * @throws SQLException If unable to query the database
   */
  @SuppressWarnings("unchecked")
  public <T extends Event> T findEvent(PointQueryParams<T> params) throws SQLException {

    T event;

    if (params.getMetadata().getType() == FloatEvent.class) {
      event = (T) findFloatEvent(params);
    } else if (params.getMetadata().getType() == IntEvent.class) {
      event = (T) findIntEvent(params);
    } else {
      event = (T) findMultiStringEvent(params);
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
      try (PreparedStatement stmt = generator.getEventPointStatement(con, params)) {
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getTimestamp()));
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            return floatFromRow(rs);
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
      try (PreparedStatement stmt = generator.getEventPointStatement(con, params)) {
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getTimestamp()));
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            return intFromRow(rs);
          } else {
            return null;
          }
        }
      }
    }
  }

  /**
   * Find a multi-string-valued event associated with the specified PointQueryParams.
   *
   * @param params The PointQueryParams
   * @return A MultiStringEvent or null if none found
   * @throws SQLException If unable to query the database
   */
  public MultiStringEvent findMultiStringEvent(PointQueryParams params) throws SQLException {
    String host = params.getMetadata().getHost();
    try (Connection con = nexus.getConnection(host)) {
      try (PreparedStatement stmt = generator.getEventPointStatement(con, params)) {
        stmt.setLong(1, TimeUtil.toMyaTimestamp(params.getTimestamp()));
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            return fromRow(rs, params.getMetadata().getSize());
          } else {
            return null;
          }
        }
      }
    }
  }
}
