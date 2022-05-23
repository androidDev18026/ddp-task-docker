package edu.auth.csd.datalab.db;

import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import edu.auth.csd.datalab.db.utils.models.MyIgnite;
import edu.auth.csd.datalab.db.utils.models.MyRedis;

class HashJoin {

    private static MyIgnite ignite;
    private static MyRedis redis;
    private static HashJoin hashJoin = null;
    private static Hashtable<Integer, String> redisHT;
    private static Hashtable<Integer, String> igniteHT;
    private static Logger logger = null;
    private static int counter = 0;

    static {
        final String logLevel = System.getProperty("logLevel") != null ? System.getProperty("logLevel") : "INFO";
        logger = Logger.getLogger(HashJoin.class.getName());
        logger.setLevel(Level.parse(logLevel));
    }

    private HashJoin(MyIgnite ignite_i, MyRedis redis_i) {
        ignite = ignite_i;
        redis = redis_i;
        ignite.createIterator();
        redis.createIterator();
    }

    public static HashJoin getInstance(final MyIgnite ignite_i, final MyRedis redis_i) {
        if (hashJoin == null) {
            hashJoin = new HashJoin(ignite_i, redis_i);
        }
        return hashJoin;
    }

    public ImmutableTriple<String, String, String> probeAndInsert(final ImmutablePair<String, String> tuple,
            Hashtable<Integer, String> htInsert,
            Hashtable<Integer, String> htProbe) {

        String probeResultKey = htProbe.get(tuple.getKey().hashCode());

        ImmutableTriple<String, String, String> resultSet = null;

        if (probeResultKey != null) {
            resultSet = ImmutableTriple.of(probeResultKey, redis.getData(probeResultKey),
                    ignite.getData(probeResultKey));
        }

        htInsert.putIfAbsent(tuple.getKey().hashCode(), tuple.getKey());

        return resultSet;
    }

    public void doPipelinedHashJoin() {
        redisHT = new Hashtable<>(10000);
        igniteHT = new Hashtable<>(10000);

        boolean readFromRedis = true;
        ImmutableTriple<String, String, String> result;

        logger.info("============================== Hash Join ==============================");
        while (redis.getIterator().hasNext() && ignite.getIterator().hasNext()) {
            if (readFromRedis) {
                result = probeAndInsert(redis.getNextTuple(), redisHT, igniteHT);
                printResult(result, "Ignite", "Redis");
            } else {
                result = probeAndInsert(ignite.getNextTuple(), igniteHT, redisHT);
                printResult(result, "Redis", "Ignite");
            }

            readFromRedis = !readFromRedis;
        }

        while (redis.getIterator().hasNext()) {
            result = probeAndInsert(redis.getNextTuple(), redisHT, igniteHT);
            printResult(result, "Ignite", "Redis");
        }

        while (ignite.getIterator().hasNext()) {
            result = probeAndInsert(ignite.getNextTuple(), igniteHT, redisHT);
            printResult(result, "Redis", "Ignite");
        }
    }

    public static void printResult(final ImmutableTriple<String, String, String> triple, final String probe,
            final String insert) {
        if (triple != null) {
            logger.info(String.format("Got match from probing %-6s and inserting %-6s => (%s, %s, %s)",probe, insert,
                triple.left,
                triple.middle, triple.right));
            ++counter;
        }
    }

    public int getCounter() {
        return counter;
    }
}
