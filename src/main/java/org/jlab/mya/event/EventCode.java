package org.jlab.mya.event;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Mya event codes. There are two types of events: updates and informational. Some informational
 * event types are also intended to represent disconnections, or gaps in event history.
 *
 * @author slominskir
 */
public enum EventCode {
  /** Update */
  UPDATE(0, "Normal channel data point"),
  /** Network Disconnection */
  NETWORK_DISCONNECTION(1, "Network disconnection"),
  /** Archiving Disabled */
  ARCHIVING_OF_CHANNEL_TURNED_OFF(2, "Archiving of channel turned off"),
  /** Archiver Shutdown */
  ARCHIVER_SHUTDOWN(3, "Archiver shutdown"),
  /** Unknown */
  UNKNOWN_UNAVAILABILTY(4, "Unknown unavailability"),
  /** NaN or Infinity */
  NAN_OR_INFINITY(5, "NaN/infinity encountered"),
  /** Origin */
  ORIGIN_OF_CHANNELS_HISTORY(16, "Origin of channel's history"),
  /** Offline */
  CHANNELS_PRIOR_DATA_MOVED_OFFLINE(32, "Channel's prior data moved offline"),
  /** Discarded */
  CHANNELS_PRIOR_DATA_DISCARDED(48, "Channel's prior data discarded"),
  /**
   * Undefined. This event code is not defined in the C++ API. It is used to indicate the lack of
   * the known state of the channel and is an "artificial" event. This is best used to simulate an
   * event that happens before the start of recorded data in the database or an event that happens
   * in the future. mySamplerStream provides a good example of this.
   */
  UNDEFINED(255, "undefined");

  private final int codeNumber;
  private final String description;
  private final boolean disconnection;

  private static final Set<EventCode> dataEventCodes =
      Collections.unmodifiableSet(
          Stream.of(
                  UPDATE,
                  ORIGIN_OF_CHANNELS_HISTORY,
                  CHANNELS_PRIOR_DATA_MOVED_OFFLINE,
                  CHANNELS_PRIOR_DATA_DISCARDED)
              .collect(Collectors.toSet()));

  /**
   * Create a new EventCode enum value with the specified description.
   *
   * @implNote MYA event code numbers are split into two orthogonal components stored in the low and
   *     high nibble. The low nibble has the event data or reason why is doesn't (disconnect event).
   *     The high nibble has the reason why there is no data prior to this event or that there is
   *     data prior to this event. MYA history for a channel should not start on an event that does
   *     not have data. Beyond that there is a convention that "normal" updates should not have
   *     messages displayed since they make up the vast bulk of all updates and are implied.
   * @param codeNumber The event code number
   * @param description The description, which is intended to match the core C++ based MYA tools
   */
  EventCode(int codeNumber, String description) {
    this.codeNumber = codeNumber;
    this.description = description;
    this.disconnection = (codeNumber & 0b1111) > 0;
  }

  /**
   * Return the code number of the EventCode.
   *
   * @return The code number
   */
  public int getCodeNumber() {
    return codeNumber;
  }

  /**
   * Return the EventCode description.
   *
   * @return The description
   */
  public String getDescription() {
    if (codeNumber > 0) {
      return description;
    } else {
      return "";
    }
  }

  /**
   * Return whether the EventCode represents a "disconnection" event.
   *
   * @return true if a disconnection event, false otherwise
   */
  public boolean isDisconnection() {
    return disconnection;
  }

  /**
   * Return all event codes that indicate there is data.
   *
   * @return Set of EventCodes
   */
  public static Set<EventCode> getDataEventCodes() {
    return dataEventCodes;
  }

  /**
   * Convert a Mya event code number (as found in the database) to enum value.
   *
   * @param number The code number
   * @return The EventCode enum value
   */
  public static EventCode fromInt(int number) {
    EventCode code;
    switch (number) {
      case 0:
        code = EventCode.UPDATE;
        break;
      case 1:
        code = EventCode.NETWORK_DISCONNECTION;
        break;
      case 2:
        code = EventCode.ARCHIVING_OF_CHANNEL_TURNED_OFF;
        break;
      case 3:
        code = EventCode.ARCHIVER_SHUTDOWN;
        break;
      case 4:
        code = EventCode.UNKNOWN_UNAVAILABILTY;
        break;
      case 5:
        code = EventCode.NAN_OR_INFINITY;
        break;
      case 16:
        code = EventCode.ORIGIN_OF_CHANNELS_HISTORY;
        break;
      case 32:
        code = EventCode.CHANNELS_PRIOR_DATA_MOVED_OFFLINE;
        break;
      case 48:
        code = EventCode.CHANNELS_PRIOR_DATA_DISCARDED;
        break;
      case 255:
        code = EventCode.UNDEFINED;
        break;
      default:
        throw new IllegalArgumentException("Unknown code number: " + number);
    }

    return code;
  }
}
