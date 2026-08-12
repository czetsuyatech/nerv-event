package com.czetsuyatech.nerv.event.spring.scheduler;

public enum SchedulerType {
  OUTBOX_DISPATCH("outbox-dispatch"), INBOX_RETRY("inbox-retry"), EVENT_RETENTION("event-retention");

  private final String tag;

  SchedulerType(String tag) {
    this.tag = tag;
  }

  public String tag() {
    return tag;
  }
}
