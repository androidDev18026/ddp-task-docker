package edu.auth.csd.datalab.db.utils.interfaces;

import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;

public interface MyDatabase {

    public String getData(String key);

    public void putData(String key, String value);

    public void close();

    public void displayAllData();

    public List<String> getAllKeys();

    public void deleteCache();

    public void createIterator();

    public int getSize();
    
    public ImmutablePair<String, String> getNextTuple();
}
