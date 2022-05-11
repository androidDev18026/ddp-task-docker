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

        Hashtable<Integer, String> htRedis = redis.getHashtable();
        Hashtable<Integer, String> htIgnite = ignite.getHashtable();

        /* check for matches hash-join
        for (int key : htRedis.keySet()) {
            if (htIgnite.containsKey(key)) {
                ++counter;
                System.out.printf("*Found match for key %d -> [Redis ] value: %-4s\t[Ignite] value: %-4s\n", key,
                        redis.getData(htRedis.get(key)), ignite.getData(htIgnite.get(key)));
            }
        }
        */
        
        // alternative
        for (int key : htRedis.keySet()) {
            for (String key_i : ignite.getAllKeys()) {
                if (key_i.hashCode() == key) {
                    ++counter;
                    System.out.printf("*Found match for key %d -> [Redis ] value: %-4s\t[Ignite] value: %-4s\n", key,
                        redis.getData(htRedis.get(key)), ignite.getData(key_i));
                }
            }
        }

        System.out.println("Found " + counter + " matches");

        ignite.close();
        redis.close();
    }
}
