package edu.auth.csd.datalab.db;

import java.util.Hashtable;
import java.util.Random;

import edu.auth.csd.datalab.db.utils.models.MyIgnite;
import edu.auth.csd.datalab.db.utils.models.MyRedis;

public class DebugQueries {

    final static String string_iter = "ITER";
    final static String string_item = "ITEMS";
    final static String string_ignite = "IGNITE_HOST";
    final static String string_redis = "REDIS_HOST";
    final static RandomString rs = new RandomString(2);

    static int counter = 0;

    public static void main(String[] args) {
        String igniteHost = (System.getenv(string_ignite) != null) ? System.getenv(string_ignite) : "HelloIgnite";
        String redisHost = (System.getenv(string_redis) != null) ? System.getenv(string_redis) : "HelloRedis";
        int iter = (System.getenv(string_iter) != null && Integer.parseInt(System.getenv(string_iter)) > 0)
                ? Integer.parseInt(System.getenv(string_iter))
                : 100;

        MyIgnite ignite = new MyIgnite(igniteHost, 10800);
        MyRedis redis = new MyRedis(redisHost, 6379);

        ignite.deleteCache();
        redis.deleteCache();

        System.out.println("Filling databases with " + iter + " elements");

        for (int i = 0; i < iter; ++i) {
            ignite.putData(rs.getRandomString(), String.valueOf(new Random().nextInt(10)));
            redis.putData(rs.getRandomString(), String.valueOf(new Random().nextInt(10)));
        }

        // System.out.println(ignite.getData("test"));
        // System.out.println(redis.getData("test"));

        ignite.displayAllData();
        redis.displayAllData();

        ignite.constructHT();
        redis.constructHT();
        
        HashJoin hashJoin = HashJoin.getInstance(ignite, redis);

        long start1 = System.currentTimeMillis();
        hashJoin.doHashJoin1();
        long dur1 = System.currentTimeMillis() - start1;

        long start2 = System.currentTimeMillis();
        hashJoin.doHashJoin2();
        long dur2 = System.currentTimeMillis() - start2;
        
        System.out.printf("HashJoin1 took %dms\n", dur1);
        System.out.printf("HashJoin2 took %dms\n", dur2);
        
        ignite.close();
        redis.close();
    }
}
