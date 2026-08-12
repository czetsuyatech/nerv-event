package com.czetsuyatech.nerv.event.core.routing;

import com.czetsuyatech.nerv.event.model.Destination;

/**
 * Resolves a logical destination at delivery time.
 */
public interface DestinationResolver {

  DestinationRoute resolve(Destination destination);
}
