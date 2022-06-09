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
    final static String strlen = "STR_LEN";
    static RandomString rs;
    
    /* 
    Fills database with random keys from the RandomString class and values    
    from 0 to 100 represented as strings
    */
    public static void fillDB(MyDatabase db, final int nElements) {
        for (int i = 0; i < nElements; ++i) {
            db.putData(rs.getRandomString(), String.valueOf(new Random().nextInt(100)));
        }
    }

    public static Logger getLogger() {

        System.setProperty(
                "java.util.logging.SimpleFormatter.format",
                "[%1$td/%1$tm/%1$ty %1$tH:%1$tM:%1$tS - %4$-7s]: %5$s %n");
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
        
        int charLength = (System.getenv(strlen) != null && Integer.parseInt(System.getenv(strlen)) > 0)
                ? Integer.parseInt(System.getenv(strlen))
                : 2;

        final Logger mainLogger = getLogger();
        // Initialize random string generator for the keys
        rs = new RandomString(charLength);
        
        // Connect to DBs'        
        MyIgnite ignite = new MyIgnite(igniteHost, 10800);
        MyRedis redis = new MyRedis(redisHost, 6379);
        
        // Delete any data residing in those databases (for debugging)
        ignite.deleteCache();
        redis.deleteCache();

        mainLogger.info("Filling Ignite with " + iterIgnite + " elements");
        mainLogger.info("Filling Redis  with " + iterRedis + " elements");
        mainLogger.info("Key length is " + charLength + " characters");
        
        fillDB(ignite, iterIgnite);
        fillDB(redis, iterRedis);

        if (mainLogger.getLevel().equals(Level.CONFIG)) {
            ignite.displayAllData();
        }
        System.out.println("-------------------------------------");
        if (mainLogger.getLevel().equals(Level.CONFIG)) {
            redis.displayAllData();
        }
        
        /* ------------------ Hash Join ---------------- */
        HashJoin hashJoin = HashJoin.getInstance(ignite, redis);

        long startHJ = System.currentTimeMillis();
        hashJoin.doPipelinedHashJoin();
        long durHJ = System.currentTimeMillis() - startHJ;

        /* ------------------ Semi Join ---------------- */
        SemiJoin semiJoin = SemiJoin.getInstance(ignite, redis);

        long startSJ = System.currentTimeMillis();
        semiJoin.doSemiJoin();
        long durSJ = System.currentTimeMillis() - startSJ;
        
        /* ------------------ Intersection BF Join ---------------- */
        IntersectionBloomFilter intersectionBF = IntersectionBloomFilter.getInstance(ignite, redis,
            Math.round((float) Math.ceil((iterRedis + iterIgnite) * 0.1f)), 0.02f);

        long startBFJ1 = System.currentTimeMillis();
        intersectionBF.doIntersectionBFJoin1();
        long durBFJ1 = System.currentTimeMillis() - startBFJ1;

        long startBFJ2 = System.currentTimeMillis();
        intersectionBF.doIntersectionBFJoin2();
        long durBFJ2 = System.currentTimeMillis() - startBFJ2;
        
        // Display the number of matching pairs and the execution time for each method
        System.out.printf("\n== Pipelined Hash Join took %dms - %d hit(s) ==\n", durHJ, hashJoin.getCounter());
        System.out.printf("== Semi Join took %dms - %d hit(s) ==\n", durSJ - 5000, semiJoin.getCounter());
        System.out.printf("== Intersection Bloom Join (Guava) took %dms - %d hit(s) ==\n", durBFJ1,
                intersectionBF.getCounter());
        System.out.printf("== Intersection Bloom Join (Mine) took %dms - %d hit(s) ==\n", durBFJ2,
                intersectionBF.getCounter());

        ignite.close();
        redis.close();
    }
}
