package com.czetsuyatech.nerv.event.starter.customconsumer;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomOrderRepository extends JpaRepository<CustomOrder, UUID> {
}
