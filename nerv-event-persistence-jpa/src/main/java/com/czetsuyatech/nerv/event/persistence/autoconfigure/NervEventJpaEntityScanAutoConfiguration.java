package com.czetsuyatech.nerv.event.persistence.autoconfigure;

import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import jakarta.persistence.EntityManagerFactory;
import java.util.LinkedHashSet;
import java.util.Set;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.persistence.autoconfigure.EntityScanPackages;
import org.springframework.context.annotation.Bean;

/**
 * Adds NERV Event entities to consumer-owned or Spring Boot default entity scanning.
 */
@AutoConfiguration(before = HibernateJpaAutoConfiguration.class)
@ConditionalOnClass(EntityManagerFactory.class)
public class NervEventJpaEntityScanAutoConfiguration {

  private static final String ENTITY_SCAN_PACKAGES_BEAN_NAME = EntityScanPackages.class.getName();

  @Bean
  static BeanDefinitionRegistryPostProcessor nervEventJpaEntityScanPackages() {
    return new BeanDefinitionRegistryPostProcessor() {
      @Override
      public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        Set<String> packageNames = new LinkedHashSet<>();
        if (!registry.containsBeanDefinition(ENTITY_SCAN_PACKAGES_BEAN_NAME)) {
          if (!(registry instanceof ConfigurableListableBeanFactory beanFactory)) {
            throw new IllegalStateException("The bean definition registry must also be a bean factory");
          }
          if (AutoConfigurationPackages.has(beanFactory)) {
            packageNames.addAll(AutoConfigurationPackages.get(beanFactory));
          }
        }
        packageNames.add(OutboxEventEntity.class.getPackageName());
        EntityScanPackages.register(registry, packageNames);
      }

      @Override
      public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
        // Entity scan packages are registered during the registry phase.
      }
    };
  }
}
