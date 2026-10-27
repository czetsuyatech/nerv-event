package com.czetsuyatech.nerv.event.persistence.persistence.config;

import com.czetsuyatech.nerv.event.persistence.NervEventPersistencePackage;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Spring Data repository configuration imported by NERV's JPA auto-configuration independently of consumer repository
 * discovery.
 */
@Configuration(proxyBeanMethods = false)
@EnableJpaRepositories(basePackageClasses = NervEventPersistencePackage.class)
public class NervEventJpaRepositoriesConfiguration {
}
