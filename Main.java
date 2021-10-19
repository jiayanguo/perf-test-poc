package perf;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Main {
    public static void main(String[] args) {

        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(300);
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("worker-task-%d").build();
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(30, 30, 1, TimeUnit.MINUTES, workQueue,
                threadFactory );

        CopyOnWriteArrayList<String> messageIdList = new CopyOnWriteArrayList<>();
        ThreadFactory daemonFactory = new ThreadFactoryBuilder().setNameFormat("daemon-task-%d").setDaemon(true).build();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2,daemonFactory);  // use 2 threads,
        // second thread should cancel itself if the first thread is still running.
        Semaphore lock = new Semaphore(1);
        scheduler.scheduleAtFixedRate(new Daemon(lock, messageIdList), 2, 2, TimeUnit.SECONDS);
        ConcurrentLinkedQueue<String> resultQueue = new ConcurrentLinkedQueue();

        long start = System.currentTimeMillis() / 1000;
        int counter = 1;
        RateLimiter limiter = RateLimiter.create(10);
        while (!shouldQuit(workQueue, start)) {
            // adjust rate limit every 5 seconds
            long duration = Instant.now().getEpochSecond() - start;
            if (duration/5 == counter && duration %5 == 0) {
                counter ++;
                if (workQueue.size() < 100) {
                    limiter.setRate(limiter.getRate() + 10);
                    log("new rate limit :" + limiter.getRate());
                }
            }
            limiter.acquire();
            poolExecutor.execute(new CreateWorker(resultQueue, messageIdList));
            poolExecutor.execute(new DeleteWorker(resultQueue));
        }

        log("shutdown now ...");
        scheduler.shutdown();
        poolExecutor.shutdown();

        try {
            if (!scheduler.awaitTermination(3000, TimeUnit.MILLISECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            poolExecutor.shutdownNow();
        }

        try {
            if (!poolExecutor.awaitTermination(20000, TimeUnit.MILLISECONDS)) {
                poolExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            poolExecutor.shutdownNow();
        }
    }


    static class CreateWorker implements Runnable {
        ConcurrentLinkedQueue<String> resultQueue;
        CopyOnWriteArrayList<String> messageIdList;

        public CreateWorker( ConcurrentLinkedQueue<String> resultQueue, CopyOnWriteArrayList<String> messageIdList) {
            this.resultQueue = resultQueue;
            this.messageIdList = messageIdList;
        }

        @Override
        public void run() {
            Random random = new Random();
            try {
//                logThread(" worker starting the task");
                Thread.sleep(random.nextInt(3000));
                String id = UUID.randomUUID().toString() ;
                logThread("created" + id);
                resultQueue.add(id);
                messageIdList.add("message" + id);
//                logThread(" worker finished the task");
            } catch (InterruptedException e) {
                logThread(e.getMessage());
            }
        }
    }

    static class DeleteWorker implements Runnable {
        ConcurrentLinkedQueue<String> resultQueue;

        public DeleteWorker( ConcurrentLinkedQueue<String> resultQueue ) {
            this.resultQueue = resultQueue;
        }

        @Override
        public void run() {

            Random random = new Random();
            try {
//                logThread(" worker starting the task");
                String id = resultQueue.poll();
                if ( id == null) {
                    logThread("result queue is empty");
                    return;
                }
                Thread.sleep(random.nextInt(3000));

                logThread("removed id " + id);
//                logThread(" worker finished the task");
            } catch (InterruptedException e) {
                logThread(e.getMessage());
            }
        }
    }


    static class Daemon implements Runnable {
        private Semaphore lock;
        CopyOnWriteArrayList<String> messageIdList;

        public Daemon(Semaphore lock, CopyOnWriteArrayList<String> messageIdList) {
            this.lock = lock;
            this.messageIdList = messageIdList;
        }

        @Override
        public void run() {
            // if can't get the lock, means the previous work is still running. cancel it.
            if (!lock.tryAcquire(1) ) {
                logThread("cancel current iteration!");
                return;
            } else {
                try {
                    Random random = new Random();
                    int time = random.nextInt(3000);
                    logThread( " running daemon thread. it will take: " + time + " " + "millisecond");

                    int size = messageIdList.size();

                    if ( size != 0) {
                        int num = random.nextInt(size);

                        while (num > 0) {
                            String id = messageIdList.remove(0);
                            logThread("remove message id: "+ id);
                        }
                    }

                    Thread.sleep(time);
                } catch (InterruptedException e) {
                    logThread(e.getMessage());
                } finally {
                    lock.release(1);
                    logThread( "unlocked");
                }
            }
        }
    }


    private static boolean shouldQuit(BlockingQueue<Runnable> workQueue, long start) {

        if (workQueue.size() == 300) {
            log("workqueue is full. we should quit :" + workQueue.size());
            return true;
        }

        if (Instant.now().getEpochSecond() - start >= 5*60) {
            log("duration expired");
            return true;
        }

        // ... check error rate
        return false;
    }

    private static void log(String message) {
        System.out.println("[" + Instant.now().toString() + "] " + message);
    }

    private static void logThread(String message) {
        System.out.println("[" + Instant.now().toString() + "] " + "[" + Thread.currentThread().getName() + "] " + message);
    }
}

