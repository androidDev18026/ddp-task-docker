package edu.auth.csd.datalab.db;

import java.util.List;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import edu.auth.csd.datalab.db.utils.models.MyIgnite;
import edu.auth.csd.datalab.db.utils.models.MyRedis;

public class IntersectionBloomFilter {

    private BloomFilter<String> bloomFilterR;
    private BloomFilter<String> bloomFilterL;
    private static MyIgnite ignite;
    private static MyRedis redis;
    private static Logger logger = null;
    private static IntersectionBloomFilter intersectionBloomFilter = null;
    private static final float FPP = 0.01f; // accept 1% false-positive rate
    static int counter = 0;

    static {
        System.setProperty(
                "java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] [%4$-7s] %5$s %n");
        final String logLevel = System.getProperty("logLevel") != null ? System.getProperty("logLevel") : "INFO";
        logger = Logger.getLogger(SemiJoin.class.getName());
        logger.setLevel(Level.parse(logLevel));
    }

    private IntersectionBloomFilter(MyIgnite ignite_i, MyRedis redis_i, int capacity) {
        ignite = ignite_i;
        redis = redis_i;
        logger.info("============================== Intersection Bloom Join ==============================");
        logger.info("Setting initial capacity to " + capacity + " elements");
        logger.info("Accepting " + FPP * 100 + "% false positive probability");
        
        bloomFilterL = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), capacity, FPP);
        bloomFilterR = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), capacity, FPP);
    }

    public static IntersectionBloomFilter getInstance(final MyIgnite ignite_i, final MyRedis redis_i,
            final int capacity) {
        if (intersectionBloomFilter == null) {
            intersectionBloomFilter = new IntersectionBloomFilter(ignite_i, redis_i, capacity);
        }
        return intersectionBloomFilter;
    }

    public static void fillBF(final List<String> keys, BloomFilter<String> bf) {
        for (final String key : keys) {
            bf.put(key);
        }
    }

    public Predicate<String> intersectionPredicateBF() {
        return bloomFilterL.and(bloomFilterR);
    }

    public void doIntersectionBFJoin() {

        List<String> redisKeys = redis.getAllKeys();
        List<String> igniteKeys = ignite.getAllKeys();

        fillBF(redisKeys, bloomFilterL);
        fillBF(igniteKeys, bloomFilterR);

        Predicate<String> intersectionBF = intersectionPredicateBF();

        redisKeys.removeIf(intersectionBF.negate());
        igniteKeys.removeIf(intersectionBF.negate());

        for (final String key : redisKeys) {
            for (final String igniteKey : igniteKeys) {
                if (igniteKey.equals(key)) {
                    logger.info(String.format("Got match => (%s, %s, %s)", key, redis.getData(key),
                            ignite.getData(key)));
                    ++counter;
                }
            }
        }
    }

    public int getCounter() {
        return counter;
    }
}
