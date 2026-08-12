package com.czetsuyatech.nerv.event.persistence.service;

import com.czetsuyatech.nerv.event.persistence.persistence.entity.InboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.InboxEventRepository;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Isolates a unique-key failure so duplicate registration can be handled in a new transaction.
 */
@Repository
@RequiredArgsConstructor
public class InboxRegistrationWriter {

  @NonNull
  private final InboxEventRepository entityRepository;

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void insert(InboxEventEntity event) {
    entityRepository.saveAndFlush(event);
  }
}
