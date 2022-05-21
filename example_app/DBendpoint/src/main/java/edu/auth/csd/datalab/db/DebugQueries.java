package edu.auth.csd.datalab.db;

import java.util.Random;

import edu.auth.csd.datalab.db.utils.models.MyIgnite;
import edu.auth.csd.datalab.db.utils.models.MyRedis;

public class DebugQueries {

    final static String string_iter = "ITER";
    final static String string_item = "ITEMS";
    final static String string_ignite = "IGNITE_HOST";
    final static String string_redis = "REDIS_HOST";
    final static RandomString rs = new RandomString(2);

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

        System.out.println("Filling 2 databases with " + iter + " elements");

        for (int i = 0; i < iter; ++i) {
            ignite.putData(rs.getRandomString(), String.valueOf(new Random().nextInt(100)));
            redis.putData(rs.getRandomString(), String.valueOf(new Random().nextInt(100)));
        }

        ignite.displayAllData();
        System.out.println("-------------------------------------");
        redis.displayAllData();

        HashJoin hashJoin = HashJoin.getInstance(ignite, redis);

        long startHJ = System.currentTimeMillis();
        hashJoin.doPipelinedHashJoin();
        long durHJ = System.currentTimeMillis() - startHJ;

        System.out.printf("\n== Pipelined Hash-Join took %dms - %d hit(s) ==\n", durHJ, hashJoin.getCounter());

        ignite.close();
        redis.close();
    }
}
