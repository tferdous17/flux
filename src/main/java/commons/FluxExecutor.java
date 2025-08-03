package commons;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FluxExecutor {
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors());

    private FluxExecutor() {} // prevent initialization

    public static ExecutorService getExecutorService() {
        return EXECUTOR_SERVICE;
    }

    public static void shutdown() {
        EXECUTOR_SERVICE.shutdown();
    }

}
