package commons;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class FluxExecutor {
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors());
    private static final ScheduledExecutorService SCHEDULER_SERVICE = Executors.newScheduledThreadPool(2);

    private FluxExecutor() {} // prevent initialization

    public static ExecutorService getExecutorService() {
        return EXECUTOR_SERVICE;
    }

    public static ScheduledExecutorService getSchedulerService() {
        return SCHEDULER_SERVICE;
    }

    public static void shutdown() {
        EXECUTOR_SERVICE.shutdown();
        SCHEDULER_SERVICE.shutdown();
    }

}
