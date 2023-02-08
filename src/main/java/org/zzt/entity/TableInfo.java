package org.zzt.entity;

import org.apache.commons.math3.util.Pair;
import org.zzt.index.Item;
import org.zzt.index.RangeItem;

import java.util.HashSet;
import java.util.Set;

public class TableInfo {
    public String name = "";
    public Item level1 = new Item();
    public Item level2 = new Item();
    public RangeItem level3 = new RangeItem();

    public Set<Edge> equalJoins = new HashSet<>();

    public String sql = "";

    public static class Edge {
        public String node1;
        public String node2;
        public Edge(String s1, String s2){
            this.node1 = s1;
            this.node2 = s2;
        }
    }

}
