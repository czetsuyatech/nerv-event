package com.czetsuyatech.nerv.event.persistence.specification;

import com.czetsuyatech.nerv.event.application.dto.InboxQuery;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.InboxEventEntity;
import com.czetsuyatech.nerv.persistence.specification.AbstractSpecificationsBuilder;
import com.czetsuyatech.nerv.persistence.specification.constant.RelationalOperators;
import org.springframework.data.jpa.domain.Specification;

/**
 * Builds the bounded inbox filters using the shared Nerv persistence specification support.
 */
public final class InboxSpecifications extends AbstractSpecificationsBuilder<InboxEventEntity> {

  private InboxSpecifications(InboxQuery query) {
    if (query.status() != null)
      equal(
          "status",
          query.status()
      );
    if (query.eventId() != null)
      equal(
          "eventId",
          query.eventId().value()
      );
    if (query.eventType() != null)
      equal(
          "eventType",
          query.eventType()
      );
    if (query.source() != null)
      equal(
          "source",
          query.source()
      );
    if (query.correlationId() != null)
      equal(
          "correlationId",
          query.correlationId()
      );
    if (query.receivedFrom() != null)
      greaterThanOrEqualTo(
          "receivedAt",
          query.receivedFrom()
      );
    if (query.receivedTo() != null)
      lessThanOrEqualTo(
          "receivedAt",
          query.receivedTo()
      );
    if (query.updatedFrom() != null)
      greaterThanOrEqualTo(
          "updatedAt",
          query.updatedFrom()
      );
    if (query.updatedTo() != null)
      lessThanOrEqualTo(
          "updatedAt",
          query.updatedTo()
      );
  }

  public static Specification<InboxEventEntity> from(InboxQuery query) {
    return new InboxSpecifications(query).build();
  }

  private void equal(
      String attribute,
      Object value
  ) {
    with(
        attribute,
        RelationalOperators.EQUAL.toString(),
        value
    );
  }

  private void greaterThanOrEqualTo(
      String attribute,
      Object value
  ) {
    with(
        attribute,
        RelationalOperators.GREATER_THAN_EQUAL.toString(),
        value
    );
  }

  private void lessThanOrEqualTo(
      String attribute,
      Object value
  ) {
    with(
        attribute,
        RelationalOperators.LESS_THAN_EQUAL.toString(),
        value
    );
  }
}
