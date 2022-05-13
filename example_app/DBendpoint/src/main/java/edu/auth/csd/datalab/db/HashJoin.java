package edu.auth.csd.datalab.db;

import java.math.BigInteger;

import edu.auth.csd.datalab.db.utils.models.MyIgnite;
import edu.auth.csd.datalab.db.utils.models.MyRedis;

class HashJoin {

    private static MyIgnite ignite;
    private static MyRedis redis;
    private static HashJoin hashJoin = null;

    private HashJoin(MyIgnite ignite_i, MyRedis redis_i) {
        ignite = ignite_i;
        redis = redis_i;
    }

    public static HashJoin getInstance(final MyIgnite ignite_i, final MyRedis redis_i) {
        if (hashJoin == null) {
            hashJoin = new HashJoin(ignite_i, redis_i);
        }
        return hashJoin;
    }

    /*
     * public void doHashJoin1(boolean... verbose) {
     * 
     * StringHash stringHash = StringHash.getInstance();
     * boolean verb = verbose.length > 0 ? verbose[0] : false;
     * long counter = 0;
     * 
     * if (redis.getHashtable().isEmpty()) {
     * System.out.println("Found empty Hashtable in redis, building it...");
     * redis.constructHT();
     * }
     * 
     * for (BigInteger key : redis.getHashtable().keySet()) {
     * for (String key_i : ignite.getAllKeys()) {
     * if (stringHash.getHash(key_i) == key) {
     * ++counter;
     * if (verb) {
     * System.out.printf("*Found match for key %d => (%s,%s)\n", key,
     * redis.getData(redis.getHashtable().get(key)), ignite.getData(key_i));
     * }
     * }
     * }
     * }
     * 
     * System.out.printf("[HashJoin1] ===> %d hits <===\n", counter);
     * }
     */

    public void doHashJoin2(boolean... verbose) {

        boolean verb = verbose.length > 0 ? verbose[0] : false;

        long counter = 0;

        if (redis.getHashtable().isEmpty()) {
            redis.constructHT();
        }

        if (ignite.getHashtable().isEmpty()) {
            ignite.constructHT();
        }

        for (BigInteger key : redis.getHashtable().keySet()) {
            if (ignite.getHashtable().containsKey(key)) {
                ++counter;
                if (verb) {
                    System.out.printf("*Found match for key %d => (%s,%s)\n", key,
                            redis.getData(redis.getHashtable().get(key)),
                            ignite.getData(ignite.getHashtable().get(key)));
                }
            }
        }

        System.out.printf("[HashJoin2] ===> %d hits <===\n", counter);
    }
}
