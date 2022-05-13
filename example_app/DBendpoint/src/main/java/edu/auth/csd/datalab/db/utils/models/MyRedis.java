package edu.auth.csd.datalab.db.utils.models;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import edu.auth.csd.datalab.db.utils.interfaces.MyDatabase;
import redis.clients.jedis.Jedis;

public class MyRedis implements MyDatabase {

    private Jedis redis;
    private Hashtable<BigInteger, String> hashtable = new Hashtable<>(100000, (float) .8);

    public MyRedis(String url, int port) {
        redis = new Jedis(url, port);
    }

    public MyRedis() {
        redis = new Jedis("localhost", 6379);
    }

    @Override
    public String getData(String key) {
        return redis.get(key);
    }

    public void deleteCache() {
        redis.flushDB();
        System.out.println("Deleted redis cache");
    }

    public Hashtable<BigInteger, String> getHashtable() {
        return hashtable;
    }

    public void setHashtable(Hashtable<BigInteger, String> hashtable) {
        this.hashtable = hashtable;
    }

    @Override
    public void putData(String key, String value) {
        redis.set(key, value);
    }

    @Override
    public void close() {
        redis.close();
        redis = null;
    }

    public void displayAllData() {
        List<String> keys = new ArrayList<>();

        redis.keys("*").forEach(s -> keys.add(s));

        keys.stream().forEach(k -> System.out.printf("Redis: Found Key: %s\n", k));
    }

    @Override
    public List<String> getAllKeys() throws NullPointerException {
        List<String> keys = new ArrayList<>();

        redis.keys("*").forEach(s -> keys.add(s));

        if (!keys.isEmpty()) {
            return keys;
        } else {
            throw new NullPointerException("Got an empty list");
        }
    }

    @Override
    public void constructHT() {
        List<String> keys = this.getAllKeys();

        try {
            MessageDigest mDigest = MessageDigest.getInstance("SHA-256");

            for (String key : keys) {
                mDigest.update(key.getBytes());
                hashtable.put(new BigInteger(mDigest.digest()), key);
                // System.out.println("[Redis] Added key " + key + " with hash value of " +
                // mDigest.digest().toString());
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

    }
}
