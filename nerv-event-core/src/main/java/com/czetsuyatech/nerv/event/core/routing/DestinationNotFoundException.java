package com.czetsuyatech.nerv.event.core.routing;

import com.czetsuyatech.nerv.event.exception.NervEventException;

/**
 * Raised when an event refers to a logical destination without a configured route.
 */
public final class DestinationNotFoundException extends NervEventException {

  public DestinationNotFoundException(String destinationName) {
    super("No route configured for destination '" + destinationName + "'");
  }
}
