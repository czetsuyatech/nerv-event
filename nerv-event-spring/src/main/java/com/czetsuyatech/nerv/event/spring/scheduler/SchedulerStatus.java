package com.czetsuyatech.nerv.event.spring.scheduler;

import java.time.Duration;
import java.time.Instant;

public record SchedulerStatus(
    SchedulerState state,
    boolean scheduled,
    boolean workInProgress,
    Instant startedAt,
    Instant lastStartedAt,
    Instant lastSuccessfulCompletionAt,
    Instant lastCycleFailureAt,
    String lastCycleFailure,
    Instant lastSchedulingFailureAt,
    String lastSchedulingFailure,
    Instant nextExecutionAt,
    long successfulCycleCount,
    long failedCycleCount,
    long schedulingFailureCount,
    Duration maximumInterval
)
{
}
