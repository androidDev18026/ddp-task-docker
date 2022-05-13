package edu.auth.csd.datalab.db.utils.models;

import edu.auth.csd.datalab.db.utils.interfaces.MyDatabase;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

public class MyIgnite implements MyDatabase {

    private IgniteClient ignite;
    private ClientCache<String, String> cache;
    private Hashtable<BigInteger, String> hashtable = new Hashtable<>(100000, (float) 0.8);

    public Hashtable<BigInteger, String> getHashtable() {
        return hashtable;
    }

    public void setHashtable(Hashtable<BigInteger, String> hashtable) {
        this.hashtable = hashtable;
    }

    public MyIgnite(String url, int port) {
        ClientConfiguration cfg = new ClientConfiguration().setAddresses(url + ":" + port);
        ignite = Ignition.startClient(cfg);
        cache = ignite.cache("MyData");
    }

    public MyIgnite() {
        ClientConfiguration cfg = new ClientConfiguration().setAddresses("localhost:10800");
        ignite = Ignition.startClient(cfg);
        cache = ignite.cache("MyData");
    }

    public void deleteCache() {
        cache.removeAll();
        System.out.println("Deleted ignite cache");
    }

    public String getData(String key) {
        return cache.get(key);
    }

    public boolean containsKey(String key) {
        return cache.containsKey(key);
    }

    public void putData(String key, String value) {
        cache.put(key, value);
    }

    public void close() {
        cache = null;
        ignite.close();
        ignite = null;
    }

    // initial implementation with simple list
    @Override
    public void displayAllData() {
        List<String> keys = new ArrayList<>();

        cache.query(new ScanQuery<>()).forEach(key -> {
            if (key.getKey() != null)
                keys.add(key.getKey().toString());
        });

        keys.stream().forEach(k -> System.out.printf("Ignite: Found Key: %s\n", k));
    }

    @Override
    public List<String> getAllKeys() throws NullPointerException {
        List<String> keys = new ArrayList<>();

        cache.query(new ScanQuery<>()).forEach(key -> {
            if (key.getKey() != null)
                keys.add(key.getKey().toString());
        });

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
                // System.out.println("[Ignite] Added key " + key + " with hash value of " +
                // mDigest.digest().toString());
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}
