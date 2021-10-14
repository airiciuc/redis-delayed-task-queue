import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.stream.Collectors;

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

        final RedisCommands<String, String> sync = client.connect().sync();
//        final CollectionType taskListType = om.getTypeFactory().constructCollectionType(ArrayList.class, Task.class);
//        final List<Task> tasks = om
//                .readValue(Main.class.getClassLoader().getResource(TASKS_CONFIG_FILE), taskListType);
//
//        new TaskScheduler(client, tasks, TASKS_LIST_NAME, SCHEDULE_ZSET_NAME).run(RUN_DURATION);
//        new TaskExecutor(client, TASKS_LIST_NAME).run(RUN_DURATION);

//        creteTask(sync);
//        isTaskUnlocked(sync);
//        isTaskLocked(sync);
        unlockTask(sync);
//        unlockAndRescheduleTask(sync);
    }

    private static void unlockAndRescheduleTask(RedisCommands<String, String> sync) {
        final String isTaskLocked = readFile("unlockAndRescheduleTask.lua");
        final Object eval = sync.eval(isTaskLocked, ScriptOutputType.MULTI,
                new String[]{"tenantId", "taskType", "taskName"}, String.valueOf(1000));
        System.out.println(eval);
    }

    private static void unlockTask(RedisCommands<String, String> sync) {
        final String isTaskLocked = readFile("unlockTask.lua");
        final Object eval = sync.eval(isTaskLocked, ScriptOutputType.MULTI,
                "tenantId", "taskType", "taskName");
        System.out.println(eval);
    }

    private static void isTaskLocked(RedisCommands<String, String> sync) {
        final String isTaskLocked = readFile("isTaskLocked.lua");
        final String sha = sync.scriptLoad(isTaskLocked);

        final Object eval = sync.evalsha(sha, ScriptOutputType.BOOLEAN,
                "tenantId", "taskType", "taskName");
        System.out.println(eval);
    }

    private static void isTaskUnlocked(RedisCommands<String, String> sync) {
        final String isTaskUnlocked = readFile("isTaskUnlocked.lua");
        final String sha = sync.scriptLoad(isTaskUnlocked);

        final Object eval = sync.evalsha(sha, ScriptOutputType.BOOLEAN,
                "tenantId", "taskType", "taskName");
        System.out.println(eval);
    }

    private static void creteTask(RedisCommands<String, String> sync) {
        final String acquireLock = readFile("createTask.lua");

        final Object eval = sync.eval(acquireLock, ScriptOutputType.MULTI,
                new String[] {"tenantId", "taskType", "taskName"},
                "details", "digest", String.valueOf(100));
        System.out.println(eval);
    }

    private static String readFile(String file) {
        final InputStream inputStream = Main.class.getClassLoader().getResourceAsStream(file);
        return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
    }

}
