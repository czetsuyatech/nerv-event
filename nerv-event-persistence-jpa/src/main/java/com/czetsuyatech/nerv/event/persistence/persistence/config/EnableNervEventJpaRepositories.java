package com.czetsuyatech.nerv.event.persistence.persistence.config;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Enables NERV Event Spring Data repositories when an application owns JPA repository configuration.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(NervEventJpaRepositoriesConfiguration.class)
public @interface EnableNervEventJpaRepositories {
}
