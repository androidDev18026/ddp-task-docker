package edu.auth.csd.datalab.db;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.auth.csd.datalab.db.utils.interfaces.MyDatabase;
import edu.auth.csd.datalab.db.utils.models.MyIgnite;
import edu.auth.csd.datalab.db.utils.models.MyRedis;

public class DebugQueries {

    final static String string_iter1 = "ITER_REDIS";
    final static String string_iter2 = "ITER_IGNITE";
    final static String string_item = "ITEMS";
    final static String string_ignite = "IGNITE_HOST";
    final static String string_redis = "REDIS_HOST";
    final static RandomString rs = new RandomString(2);

    public static void fillDB(MyDatabase db, final int nElements) {
        for (int i = 0; i < nElements; ++i) {
            db.putData(rs.getRandomString(), String.valueOf(new Random().nextInt(100)));
        }
    }

    public static Logger getLogger() {

        System.setProperty(
                "java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT [%4$-7s] %5$s %n");
        final String logLevel = System.getProperty("logLevel") != null ? System.getProperty("logLevel") : "INFO";
        final Logger logger = Logger.getLogger(DebugQueries.class.getName());
        logger.setLevel(Level.parse(logLevel));

        return logger;
    }

    public static void main(String[] args) {
        String igniteHost = (System.getenv(string_ignite) != null) ? System.getenv(string_ignite) : "HelloIgnite";
        String redisHost = (System.getenv(string_redis) != null) ? System.getenv(string_redis) : "HelloRedis";
        int iterRedis = (System.getenv(string_iter1) != null && Integer.parseInt(System.getenv(string_iter1)) > 0)
                ? Integer.parseInt(System.getenv(string_iter1))
                : 100;
        int iterIgnite = (System.getenv(string_iter2) != null && Integer.parseInt(System.getenv(string_iter2)) > 0)
                ? Integer.parseInt(System.getenv(string_iter2))
                : 100;

        Logger mainLogger = getLogger();

        MyIgnite ignite = new MyIgnite(igniteHost, 10800);
        MyRedis redis = new MyRedis(redisHost, 6379);

        ignite.deleteCache();
        redis.deleteCache();

        mainLogger.info("Filling Ignite with " + iterIgnite + " elements");
        mainLogger.info("Filling Redis  with " + iterRedis + " elements");

        fillDB(ignite, iterIgnite);
        fillDB(redis, iterRedis);

        ignite.displayAllData();
        System.out.println("-------------------------------------");
        redis.displayAllData();

        HashJoin hashJoin = HashJoin.getInstance(ignite, redis);

        long startHJ = System.currentTimeMillis();
        hashJoin.doPipelinedHashJoin();
        long durHJ = System.currentTimeMillis() - startHJ;

        SemiJoin semiJoin = SemiJoin.getInstance(ignite, redis);

        long startSJ = System.currentTimeMillis();
        semiJoin.doSemiJoin();
        long durSJ = System.currentTimeMillis() - startSJ;

        IntersectionBloomFilter intersectionBF = IntersectionBloomFilter.getInstance(ignite, redis,
            Math.round((iterRedis + iterIgnite) * 0.1f));

        long startBFJ = System.currentTimeMillis();
        intersectionBF.doIntersectionBFJoin();
        long durBFJ = System.currentTimeMillis() - startBFJ;

        System.out.printf("\n== Pipelined Hash Join took %dms - %d hit(s) ==\n", durHJ, hashJoin.getCounter());
        System.out.printf("== Semi Join took %dms - %d hit(s) ==\n", durSJ - 5000, semiJoin.getCounter());
        System.out.printf("== Intersection Bloom Join took %dms - %d hit(s) ==\n", durBFJ,
                intersectionBF.getCounter());

        ignite.close();
        redis.close();
    }
}
