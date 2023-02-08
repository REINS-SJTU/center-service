package org.zzt.index;

import java.util.HashSet;
import java.util.Set;

public class Item {
    public Set<String> data;
    public Item(Set<String> data) {
        this.data = data;
    }
    public Item() {
        this.data = new HashSet<>();
    }

    public int size() {
        return data.size();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Item) {
            Item b = (Item) o;
            Set<String> data1 = this.data, data2 = b.data;
            return data1.size() == data2.size() && data1.containsAll(data2);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
