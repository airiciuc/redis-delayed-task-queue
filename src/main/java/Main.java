import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final String REDIS_CONFIG_FILE = "redis.yml";
    private static final String TASKS_CONFIG_FILE = "tasks.yml";
    private static final String TASKS_LIST_NAME = "tasks";
    private static final String SCHEDULE_ZSET_NAME = "schedules";
    private static final Duration RUN_DURATION = Duration.ofSeconds(30);

    public static void main(String[] args) throws IOException {
        final ObjectMapper om = new ObjectMapper(new YAMLFactory());
        final RedisConfig redisConfig = om
                .readValue(Main.class.getClassLoader().getResource(REDIS_CONFIG_FILE), RedisConfig.class);

        final RedisClient client = RedisClient.create(RedisURI.builder()
                .withHost(redisConfig.getHost())
                .withPort(redisConfig.getPort())
                .build());

        final CollectionType taskListType = om.getTypeFactory().constructCollectionType(ArrayList.class, Task.class);
        final List<Task> tasks = om
                .readValue(Main.class.getClassLoader().getResource(TASKS_CONFIG_FILE), taskListType);

        new TaskScheduler(client, tasks, TASKS_LIST_NAME, SCHEDULE_ZSET_NAME).run(RUN_DURATION);
        new TaskExecutor(client, TASKS_LIST_NAME).run(RUN_DURATION);
    }

}
