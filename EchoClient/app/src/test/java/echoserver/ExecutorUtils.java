package echoserver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import static echoserver.util.MyLogger.log;

public abstract class ExecutorUtils {
    public static void printState(ExecutorService executorService) {
        if (executorService instanceof ThreadPoolExecutor poolExecutor) {
            int pool = poolExecutor.getPoolSize();
            int active = poolExecutor.getActiveCount();
            int queuedTasks = poolExecutor.getQueue().size();
            long completedTask = poolExecutor.getCompletedTaskCount();
            log("[pool=" + pool + ", active=" + active + ", queuedTasks=" +
                    queuedTasks + ", completedTasks=" + completedTask + "]");
        } else {
            log(executorService);
        }
    }
}