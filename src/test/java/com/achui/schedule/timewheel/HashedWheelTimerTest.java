package com.achui.schedule.timewheel;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

class HashedWheelTimerTest {

    @Test
    void schedule() throws Exception {
        HashedWheelTimer timer = new HashedWheelTimer(1, 100);
        List<TimerFuture> futures = Lists.newLinkedList();

        for (int i = 0; i < 1000; i++) {
            String name = "Task" + i;
            long nowMS = System.currentTimeMillis();
            long delayMS = ThreadLocalRandom.current().nextInt(60000);
            long targetTime = nowMS + delayMS;

            TimerTask timerTask = () -> {
                System.out.println("============= " + name + "============= ");
                System.out.println("ThreadInfo:" + Thread.currentThread().getName());
                System.out.println("expectTime:" + targetTime);
                System.out.println("currentTime:" + System.currentTimeMillis());
                System.out.println("deviation:" + (System.currentTimeMillis() - targetTime));
                System.out.println("============= " + name + "============= ");
            };
            futures.add(timer.schedule(timerTask, delayMS, TimeUnit.MILLISECONDS));
        }


        // 随机取消
//        futures.forEach(future -> {
//
//            int x = ThreadLocalRandom.current().nextInt(2);
//            if (x == 1) {
//                future.cancel();
//            }
//
//        });
        Thread.sleep(1000);
        // 关闭
        System.out.println(timer.stop().size());
        System.out.println("Finished！");

        Thread.sleep(277777777);
    }


}