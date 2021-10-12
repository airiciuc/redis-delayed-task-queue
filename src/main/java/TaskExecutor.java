import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Duration;

public class TaskExecutor {

    private static final long POLL_TIMEOUT_S = 5;

    private final RedisClient client;
    private final String tasksListName;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TaskExecutor(RedisClient client, String tasksListName) {
        this.client = client;
        this.tasksListName = tasksListName;
    }

    public void run(Duration runDuration) {
        new Thread(() -> pollTasks(runDuration)).start();
    }

    private void pollTasks(Duration runDuration) {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisCommands<String, String> sync = connection.sync();
            final long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < runDuration.toMillis()) {
                final KeyValue<String, String> json = sync.blpop(POLL_TIMEOUT_S, tasksListName);
                if (json != null) {
                    final Task task = objectMapper.readValue(json.getValue(), Task.class);
                    System.out.println("Executing task: " + task.getName());
                }
            }
        } catch (JsonProcessingException ex) {
            System.out.println("Failed to deserialzie task: " + ex.getMessage());
        }
    }
}
