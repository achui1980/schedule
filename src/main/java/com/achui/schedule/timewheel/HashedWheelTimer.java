package com.achui.schedule.timewheel;

import com.achui.schedule.utils.CommonUtils;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * WheelTimer, Minimum precision 1ms
 */

@Slf4j
public class HashedWheelTimer implements Timer {

    /**
     * 时间间隔：时间轮，走过一个刻度需要多长时间
     */
    private final long tickDuration;

    /**
     * 时间轮，每个刻度上挂载的任务桶
     */
    private final HashedWheelBucket[] wheel;

    private final int mask;

    /**
     * 时间轮模拟指针,移动
     */

    private final Indicator indicator;

    /**
     * 时间轮启动时间
     */
    private final long startTime;

    private final Queue<HashedWheelTimerFuture> waitingTasks = Queues.newLinkedBlockingQueue();
    private final Queue<HashedWheelTimerFuture> canceledTasks = Queues.newLinkedBlockingQueue();


    /**
     * @param tickDuration:  间隔时间
     * @param ticksPerWheel: 时间轮的槽数(刻度)数量
     */
    public HashedWheelTimer(long tickDuration, int ticksPerWheel) {
        this.tickDuration = tickDuration;
        // 初始化轮盘，大小格式化为2的N次，可以使用 & 代替取余
        int ticksNum = CommonUtils.formatSize(ticksPerWheel);
        this.wheel = new HashedWheelBucket[ticksNum];
        for (int i = 0; i < ticksNum; i++) {
            wheel[i] = new HashedWheelBucket();
        }
        this.mask = wheel.length - 1;
        this.startTime = System.currentTimeMillis();

        //启动后台线程，进行指针移动
        this.indicator = new Indicator();
        new Thread(indicator, "HashedWheelTimer-Indicator").start();
    }


    @Override
    public TimerFuture schedule(TimerTask task, long delay, TimeUnit timeUnit) {
        long targetTime = System.currentTimeMillis() + timeUnit.toMillis(delay);
        HashedWheelTimerFuture timerFuture = new HashedWheelTimerFuture(task, targetTime);
        if (delay <= 0) {
            runTask(timerFuture);
            return timerFuture;
        }
        waitingTasks.add(timerFuture);
        return timerFuture;
    }


    @Override
    public Set<TimerTask> stop() {
        indicator.stop.set(true);

        return indicator.getUnprocessedTasks();
    }

    private void runTask(HashedWheelTimerFuture timerFuture) {
        timerFuture.status = HashedWheelTimerFuture.RUNNING;
        timerFuture.timerTask.run();
    }

    private class Indicator implements Runnable {
        private long tick;
        private final AtomicBoolean stop = new AtomicBoolean(false);
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void run() {
            while (!stop.get()) {
                //1. 将队列任务推入时间轮
                pushTaskToBucket();
                //2. 处理取消的任务
                processCanceledTask();
                //3. 推进时间指向下一刻
                tickTrack();
                //4. 执行任务
                int currentIndex = (int) (tick & mask);
                HashedWheelBucket bucket = wheel[currentIndex];
                bucket.expireTimerTasks(tick);
                tick++;

            }
            latch.countDown();
        }

        /**
         * 模拟指针转动，当返回时指针已经转到了下一个刻度
         */
        private void tickTrack() {
            long nextTime = startTime + (tick + 1) * tickDuration;
            long sleepTime = nextTime - System.currentTimeMillis();
            System.out.println("sleepTime" + sleepTime);
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ignore) {

                }
            }
        }

        /**
         * 处理被取消的任务
         */
        private void processCanceledTask() {
            while (true) {
                HashedWheelTimerFuture canceledTask = canceledTasks.poll();
                if (canceledTask == null) {
                    return;
                }
                // 从链表中删除该任务（bucket为null说明还没被正式推入时间格中，不需要处理）
                if (canceledTask.bucket != null) {
                    canceledTask.bucket.remove(canceledTask);
                }
            }
        }

        /**
         * 将任务推入时间轮
         */
        private void pushTaskToBucket() {
            while (true) {
                HashedWheelTimerFuture timerTask = waitingTasks.poll();
                if (timerTask == null) {
                    return;
                }
                long offset = timerTask.targetTime - startTime;
                timerTask.totalTicks = offset / tickDuration;
                //计算时间槽的下标索引
                int index = (int) (timerTask.totalTicks & mask);
                //找到指定的时间槽对应的任务桶
                HashedWheelBucket bucket = wheel[index];
                //维护一个任务桶的引用，用于快速删除任务
                timerTask.bucket = bucket;
                //将等待任务推入任务桶
                if (timerTask.status == HashedWheelTimerFuture.WAITING) {
                    bucket.add(timerTask);
                }
            }
        }

        public Set<TimerTask> getUnprocessedTasks() {
            try {
                latch.await();
            } catch (Exception ignore) {
            }
            Set<TimerTask> tasks = Sets.newHashSet();
            Consumer<HashedWheelTimerFuture> consumer = hashedWheelTimerFuture -> {
                if (hashedWheelTimerFuture.status == HashedWheelTimerFuture.WAITING) {
                    tasks.add(hashedWheelTimerFuture.timerTask);
                }
            };
            // 遍历等待队列的所有任务
            waitingTasks.forEach(consumer);
            // 遍历已经推入时间轮的所有等待任务
            for (HashedWheelBucket bucket : wheel) {
                bucket.forEach(consumer);
            }
            return tasks;
        }
    }

    /**
     * 时间格（本质就是链表，维护了这个时刻可能需要执行的所有任务）
     */
    private final class HashedWheelBucket extends LinkedList<HashedWheelTimerFuture> {


        public void expireTimerTasks(long currentTick) {
            removeIf(hashedWheelTimerFuture -> {
                // processCanceledTasks 后外部操作取消任务会导致 BUCKET 中仍存在 CANCELED 任务的情况
                if (hashedWheelTimerFuture.status == HashedWheelTimerFuture.CANCELED) {
                    return true;
                }
                if (hashedWheelTimerFuture.status != HashedWheelTimerFuture.WAITING) {
                    log.warn("[HashedWheelTimer] impossible, please fix the bug");
                    return true;
                }

                // 进行本轮调度
                if (hashedWheelTimerFuture.totalTicks <= currentTick) {
                    if (hashedWheelTimerFuture.totalTicks < currentTick) {
                        log.warn("[HashedWheelTimer] timerFuture.totalTicks < currentTick, please fix the bug");
                    }

                    //执行任务
                    try {
                        // 提交执行
                        runTask(hashedWheelTimerFuture);
                    } catch (Exception ignore) {
                    } finally {
                        hashedWheelTimerFuture.status = HashedWheelTimerFuture.FINISHED;
                    }
                }

                return false;

            });

        }
    }

    private final class HashedWheelTimerFuture implements TimerFuture {
        // 预期执行时间
        private final long targetTime;
        private final TimerTask timerTask;
        // 时间指针需要走的总步数
        private long totalTicks;

        //所属时间个的任务桶，用于快速删除任务
        private HashedWheelBucket bucket;

        /**
         * 当前状态
         * 0:初始化等待中， 1：运行中， 2：完成， 3：取消
         */
        private int status;

        private static final int WAITING = 0;
        private static final int RUNNING = 1;
        private static final int FINISHED = 2;
        private static final int CANCELED = 3;

        public HashedWheelTimerFuture(TimerTask task, long targetTime) {
            this.timerTask = task;
            this.targetTime = targetTime;
            this.status = WAITING;
        }

        @Override
        public TimerTask getTask() {
            return this.timerTask;
        }

        @Override
        public boolean cancel() {
            if (status == WAITING) {
                status = CANCELED;
                canceledTasks.add(this);
                return true;
            }
            return false;
        }

        @Override
        public boolean isCancelled() {
            return this.status == CANCELED;
        }

        @Override
        public boolean isDone() {
            return this.status == FINISHED;
        }
    }
}
