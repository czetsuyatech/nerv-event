package com.czetsuyatech.nerv.event.spring.scheduler;

public interface SchedulerStatusProvider {

  SchedulerType schedulerType();

  SchedulerStatus status();
}
