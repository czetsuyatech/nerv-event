package com.czetsuyatech.nerv.event.starter.customconsumer;

import com.czetsuyatech.nerv.event.persistence.persistence.config.EnableNervEventJpaRepositories;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EnableJpaRepositories(basePackageClasses = CustomOrderRepository.class)
@EnableNervEventJpaRepositories
public class CustomJpaConsumerApplication {
}
