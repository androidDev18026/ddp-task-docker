package edu.auth.csd.datalab.db.utils.interfaces;

import java.util.List;

public interface MyDatabase {

    public String getData(String key);
    public void putData(String key, String value);
    public void close();
    public void displayAllData();
    public List<String> getAllKeys();
    public void constructHT();
    public void deleteCache();
}
