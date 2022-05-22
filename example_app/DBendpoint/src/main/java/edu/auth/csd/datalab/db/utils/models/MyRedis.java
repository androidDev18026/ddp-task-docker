package edu.auth.csd.datalab.db.utils.models;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.auth.csd.datalab.db.utils.interfaces.MyDatabase;
import redis.clients.jedis.Jedis;

public class MyRedis implements MyDatabase {

    private Jedis redis;
    private Iterator<String> iterator;

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

    @Override
    public void putData(String key, String value) {
        redis.set(key, value);
    }

    @Override
    public void close() {
        redis.close();
        redis = null;
    }

    public Iterator<String> getIterator() {
        return iterator;
    }

    public void displayAllData() {
        List<String> keys = new ArrayList<>();

        redis.keys("*").forEach(s -> keys.add(s));

        keys.stream().forEach(k -> System.out.printf("[Redis ] Found Key-Value pair: (%s,%s)\n", k, getData(k)));
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
    public int getSize() {
        return this.getAllKeys().size();
    }
    
    @Override
    public void createIterator() {
        iterator = redis.keys("*").iterator();
    }

    @Override
    public ImmutablePair<String, String> getNextTuple() {
        String currentKey;

        if (iterator.hasNext()) {
            currentKey = iterator.next();
            return ImmutablePair.of(currentKey, this.getData(currentKey));
        }
        return null;
    }
}
