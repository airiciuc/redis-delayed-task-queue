import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TaskScheduler {

    private final RedisClient client;
    private final List<Task> tasks;
    private final String tasksListName;
    private final String scheduleZsetName;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TaskScheduler(RedisClient client, List<Task> tasks, String tasksListName, String scheduleZsetName) {
        this.client = client;
        this.tasks = tasks;
        this.tasksListName = tasksListName;
        this.scheduleZsetName = scheduleZsetName;
    }

    public void run(Duration runDuration) {
        new Thread(() -> delayTasks(runDuration)).start();
    }

    private void delayTasks(Duration runDuration) {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisCommands<String, String> sync = connection.sync();

            //put all tasks on zset
            tasks.forEach(task -> sync.zadd(scheduleZsetName, getNextRun(task), toJson(task)));

            final long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < runDuration.toMillis()) {
                final List<ScoredValue<String>> items = sync.zrangeWithScores(scheduleZsetName, 0, 0);
                if (items.isEmpty() || items.get(0).getScore() > System.currentTimeMillis()) {
                    //no items or too early, do nothing
                    Thread.sleep(1000);
                } else {
                    //found one item
                    sync.zrem(scheduleZsetName, items.get(0).getValue());
                    Task task = objectMapper.readValue(items.get(0).getValue(), Task.class);
                    sync.zadd(scheduleZsetName, getNextRun(task), items.get(0).getValue());
                    sync.rpush(tasksListName, items.get(0).getValue());
                }
            }

            //cleanup
            sync.del(tasksListName, scheduleZsetName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (JsonProcessingException ex) {
            System.out.println("Failed to parse json: " + ex.getMessage());
        }
    }

    private String toJson(Task task) {
        try {
            return objectMapper.writeValueAsString(task);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("Failed to serialize task", ex);
        }
    }

    private long getNextRun(Task task) {
        return System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(task.getPeriod());
    }
}
