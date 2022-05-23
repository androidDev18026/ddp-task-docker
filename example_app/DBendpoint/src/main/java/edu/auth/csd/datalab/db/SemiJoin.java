package edu.auth.csd.datalab.db;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.auth.csd.datalab.db.utils.interfaces.MyDatabase;
import edu.auth.csd.datalab.db.utils.models.MyIgnite;
import edu.auth.csd.datalab.db.utils.models.MyRedis;

class SemiJoin {

    private static MyIgnite ignite;
    private static MyRedis redis;
    private static Logger logger = null;
    private static SemiJoin semiJoin = null;
    private static int counter = 0;
    
    static {
        final String logLevel = System.getProperty("logLevel") != null ? System.getProperty("logLevel") : "INFO";
        logger = Logger.getLogger(HashJoin.class.getName());
        logger.setLevel(Level.parse(logLevel));
    }

    private SemiJoin(MyIgnite ignite_i, MyRedis redis_i) {
        ignite = ignite_i;
        redis = redis_i;
    }

    public static SemiJoin getInstance(final MyIgnite ignite_i, final MyRedis redis_i) {
        if (semiJoin == null) {
            semiJoin = new SemiJoin(ignite_i, redis_i);
        }
        return semiJoin;
    }

    static MyDatabase getLargest() {
        if (ignite.getSize() >= redis.getSize()) {
            logger.info("Ignite is the largest");
            return ignite;
        }
        logger.info("Redis is the largest");
        return redis;
    }

    public void doSemiJoin() {
        logger.info("============================== Semi Join ==============================");
        MyDatabase S = getLargest();
        MyDatabase R = S.equals(ignite) ? redis : ignite;

        // Get keys of large relationship
        List<String> keys_S = S.getAllKeys();

        // Simulate transfer of Π(Α)
        logger.info("Transferring S' to Site 1...");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Semi-Join R'
        List<ImmutablePair<String, String>> R1 = new ArrayList<>(10000);
        for (final String key_R : R.getAllKeys()) {
            for (final String key_S : keys_S) {
                if (key_R.equals(key_S)) {
                    R1.add(ImmutablePair.of(key_R, R.getData(key_R)));
                }
            }
        }

        // Simulate transfer of R'
        logger.info("Transferring R' to Site 2...");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Natural-Join R' ⋈(A) S
        for (final ImmutablePair<String, String> R1_tuple : R1) {
            for (final String key_S : keys_S) {
                if (R1_tuple.getKey().equals(key_S)) {
                    logger.info(String.format("Got match => (%s, %s, %s)", R1_tuple.getKey(), R1_tuple.getValue(),
                            S.getData(R1_tuple.getKey())));
                    ++counter;
                }
            }
        }
    }

    public int getCounter() {
        return counter;
    }
}
