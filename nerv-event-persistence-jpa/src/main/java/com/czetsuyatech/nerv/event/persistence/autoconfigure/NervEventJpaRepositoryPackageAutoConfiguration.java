package com.czetsuyatech.nerv.event.persistence.autoconfigure;

import com.czetsuyatech.nerv.event.persistence.NervEventPersistencePackage;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.data.jpa.autoconfigure.DataJpaRepositoriesAutoConfiguration;

/**
 * Contributes NERV Event's Spring Data repository package to Boot's normal repository discovery.
 */
@AutoConfiguration(before = DataJpaRepositoriesAutoConfiguration.class)
@AutoConfigurationPackage(basePackageClasses = NervEventPersistencePackage.class)
@ConditionalOnClass(EntityManagerFactory.class)
public class NervEventJpaRepositoryPackageAutoConfiguration {
}
