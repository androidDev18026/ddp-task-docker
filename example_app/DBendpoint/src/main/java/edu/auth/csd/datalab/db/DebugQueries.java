package edu.auth.csd.datalab.db;

import edu.auth.csd.datalab.db.utils.models.MyIgnite;
import edu.auth.csd.datalab.db.utils.models.MyRedis;

public class DebugQueries {

    final static String string_iter = "ITER";
    final static String string_item = "ITEMS";
    final static String string_ignite = "IGNITE_HOST";
    final static String string_redis = "REDIS_HOST";

    public static void main(String[] args) {
        String igniteHost = (System.getenv(string_ignite) != null) ? System.getenv(string_ignite) : "HelloIgnite";
        String redisHost = (System.getenv(string_redis) != null) ? System.getenv(string_redis) : "HelloRedis";

        MyIgnite ignite = new MyIgnite(igniteHost, 10800);
        MyRedis redis = new MyRedis(redisHost, 6379);

        ignite.putData("test1", "1");
        redis.putData("test1", "2");
        ignite.putData("test2", "1");
        redis.putData("test2", "2");
        ignite.putData("test3", "1");
        redis.putData("test3", "2");
        // System.out.println(ignite.getData("test"));
        // System.out.println(redis.getData("test"));
        ignite.getAllData();
        redis.getAllData();

        ignite.close();
        redis.close();
    }

}
