package org.github.atyagi.structures;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AutoIncrementingHashMap implements Serializable {

    private static final String UNKNOWN = "?";

    private Map<String, Integer> internalMap = new ConcurrentHashMap<>();

    private final AtomicInteger internalId = new AtomicInteger(-1);

    public boolean add(String key) {
        if (UNKNOWN.equals(key) || internalMap.containsKey(key)) {
            return false;
        } else {
            internalMap.put(key, internalId.incrementAndGet());
            return true;
        }
    }

    public Integer get(String key) throws IllegalAccessException {
        if (UNKNOWN.equals(key)) {
            return 0;
        } else if (internalMap.containsKey(key)) {
            return internalMap.get(key);
        } else {
            throw new IllegalAccessException("The key does not exist in the map");
        }
    }

    public Integer addAndGet(String key) {
        this.add(key);
        try {
            return this.get(key);
        } catch (IllegalAccessException e) {
            return 0;
        }
    }

}
