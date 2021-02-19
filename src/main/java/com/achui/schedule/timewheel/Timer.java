package com.achui.schedule.timewheel;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * scheduler
 */
public interface Timer {

    /**
     * schedule a task
     */
    TimerFuture schedule(TimerTask task, long delay, TimeUnit timeUnit);


    /**
     * stop all task
     * @return
     */
    Set<TimerTask> stop();
}
