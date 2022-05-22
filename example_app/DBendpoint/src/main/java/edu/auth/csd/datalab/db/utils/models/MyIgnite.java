package edu.auth.csd.datalab.db.utils.models;

import edu.auth.csd.datalab.db.utils.interfaces.MyDatabase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.cache.Cache.Entry;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

public class MyIgnite implements MyDatabase {

    private IgniteClient ignite;
    private ClientCache<String, String> cache;
    private Iterator<Entry<Object, Object>> iterator;

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
        try {
            cache.clear();
        } catch (ClientException e) {
            e.printStackTrace();
        }
        System.out.println("Deleted ignite cache");
    }

    public String getData(String key) {
        return cache.get(key);
    }

    public Iterator<Entry<Object, Object>> getIterator() {
        return iterator;
    }

    public boolean containsKey(String key) {
        return cache.containsKey(key);
    }

    public void putData(String key, String value) {
        // No updates on keys allowed
        cache.putIfAbsent(key, value);
    }

    public void close() {
        cache = null;
        ignite.close();
        ignite = null;
    }

    @Override
    public void createIterator() {
        iterator = cache.query(new ScanQuery<>()).iterator();
    }

    // initial implementation with simple list
    @Override
    public void displayAllData() {
        List<String> keys = new ArrayList<>();

        cache.query(new ScanQuery<>()).forEach(key -> {
            if (key.getKey() != null)
                keys.add(key.getKey().toString());
        });

        keys.stream().forEach(k -> System.out.printf("[Ignite] Found Key-Value pair: (%s,%s)\n", k, getData(k)));
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
    public int getSize() {
        return this.getAllKeys().size();
    }

    @Override
    public ImmutablePair<String, String> getNextTuple() {

        Entry<Object, Object> currentTuple;

        if (iterator.hasNext()) {
            currentTuple = iterator.next();
            return ImmutablePair.of(currentTuple.getKey().toString(), currentTuple.getValue().toString());
        }
        return null;
    }
}
