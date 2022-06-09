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
        /* Create an iterator to fetch key-value pairs
           from each DB in a streaming manner */ 
        ignite.createIterator();
        redis.createIterator();
    }

    public static HashJoin getInstance(final MyIgnite ignite_i, final MyRedis redis_i) {
        if (hashJoin == null) {
            hashJoin = new HashJoin(ignite_i, redis_i);
        }
        return hashJoin;
    }
    
    /* 
    This is an auxiliary method that checks if the probing hash table already has a specific key.
    If it is not present, we insert it using the insertion hash table.
    Finally, in any case, return the result of the join operation, even if it is empty.
    */
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
        // Initialize two empty hash tables, one for each DB
        redisHT = new Hashtable<>(10000);
        igniteHT = new Hashtable<>(10000);
        
        /*
        Here we define the strategy to draw inputs, simulating a streaming scenario.
        We start reading from Redis, though we will alternate between the two.
        */
        boolean readFromRedis = true;
        ImmutableTriple<String, String, String> result;

        logger.info("============================== Hash Join ==============================");
        // While both inputs have more elements 
        while (redis.getIterator().hasNext() && ignite.getIterator().hasNext()) {
            // If we read from Redis
            if (readFromRedis) {
                /*
                Get the next tuple from Redis and probe Ignite, finally insert to Redis'
                hash table
                */
                result = probeAndInsert(redis.getNextTuple(), redisHT, igniteHT);
                printResult(result, "Ignite", "Redis");
            // If we read from Ignite
            } else {
                /*
                Get the next tuple from Ignite and probe Redis, finally insert to Ignites
                hash table
                */
                result = probeAndInsert(ignite.getNextTuple(), igniteHT, redisHT);
                printResult(result, "Redis", "Ignite");
            }
            /*
            To simulate a pipelined streaming scenario we change the order of the
            reads by alternating between the 2 DB's after each read operation
            */
            readFromRedis = !readFromRedis;
        }
        
        /*
        Since we have exited the loop above, one of the inputs was exhausted,
        which can happen in a streaming case. 
        */
        
        // First check if Redis has more elements
        while (redis.getIterator().hasNext()) {
            // Probe Ignite to search for matches and add to Redis hash table
            result = probeAndInsert(redis.getNextTuple(), redisHT, igniteHT);
            printResult(result, "Ignite", "Redis");
        }
        
        // Redis was empty, so we get elements from Iginte (if any)
        while (ignite.getIterator().hasNext()) {
            // Probe Redis to search for matches and add to Ignites hash table
            result = probeAndInsert(ignite.getNextTuple(), igniteHT, redisHT);
            printResult(result, "Redis", "Ignite");
        }
        
        // All input are exhausted
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
