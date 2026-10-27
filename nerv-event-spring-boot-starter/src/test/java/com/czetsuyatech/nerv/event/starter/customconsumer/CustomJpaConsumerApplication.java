package com.czetsuyatech.nerv.event.starter.customconsumer;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackageClasses = CustomOrder.class)
@EnableJpaRepositories(basePackageClasses = CustomOrderRepository.class)
public class CustomJpaConsumerApplication {
}
