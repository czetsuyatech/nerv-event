package com.czetsuyatech.nerv.event.starter.consumer;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

  @Id
  private UUID id;
}
