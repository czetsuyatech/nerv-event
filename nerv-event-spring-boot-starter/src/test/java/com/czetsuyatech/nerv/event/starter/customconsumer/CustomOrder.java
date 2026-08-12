package com.czetsuyatech.nerv.event.starter.customconsumer;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;

@Entity
@Table(name = "custom_orders")
public class CustomOrder {

  @Id
  private UUID id;
}
