package org.zzt.index;

import org.apache.commons.math3.util.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class LatticeIndex {
    public Set<Item> items;
    public List<Pair<Item, Item>> R;
    public Map<Item, Set<Item>> adj;
    public Map<Item, Set<Item>> reverseAdj;
    private Set<Item> src;
    private Set<Item> dst;

    public LatticeIndex() {
        items = new HashSet<>();
    }

    public void add(Item item) {
        items.add(item);
    }


    public List<Pair<Item, Item>> findR() {
        if (R != null) {
            return R;
        }
        List<Pair<Item, Item>> ret = new ArrayList<>();

        List<Item> list = new ArrayList<>(items);
        int len = list.size();
        for (int i = 0; i < len; ++i) {
            for (int j = i + 1; j < len; ++j) {
                Item it1 = list.get(i), it2 = list.get(j);
                // TODO exactly same case
                if (isSubset(it1, it2)) {
                    ret.add(new Pair<>(it1, it2));
                }
                if (isSubset(it2, it1)) {
                    ret.add(new Pair<>(it2, it1));
                }
            }
        }
        R = ret;
        return ret;
    }

    public Map<Item, Set<Item>> init() {
        if (adj != null) {
            return adj;
        }
        Map<Item, Set<Item>> ret = new HashMap<>();

        List<Pair<Item, Item>> R = findR();
        for(Pair p: R) {
            Item src = (Item) p.getKey(), dst = (Item) p.getValue();
            ret.computeIfAbsent(src, key -> new HashSet<>());
            ret.get(src).add(dst);
        }
        adj = ret;
        // source nodes
        Set<Item> in = R.stream().map(Pair::getValue).collect(Collectors.toSet());
        src = new HashSet<>(items);
        src.removeAll(in);
        // dst nodes
        dst = new HashSet<>(items);
        dst.removeAll(adj.keySet());
        return ret;
    }

    public void deleteTransitive() {
        Map<Item, Set<Item>> adj = init();
        // find all path
        Set<List<Item>> paths = new HashSet<>();

        // find all path
        for (Item start: src) {
            // dfs
            Queue<Pair<Item, List<Item>>> q = new LinkedList<>();
            q.add(new Pair<>(start, new ArrayList<Item>(){{add(start);}}));
            while (!q.isEmpty()) {
                Pair<Item, List<Item>> cur = q.poll();
                Item node = cur.getKey();
                List<Item> p = cur.getValue();
                // finish
//                if (dst.contains(cur.getKey())) {
//                    paths.add(p);
//                }
                paths.add(p);
                Set<Item> next = adj.getOrDefault(node, new HashSet<>());
                for (Item n: next) {
                    List<Item> path = new ArrayList<>(p);
                    path.add(n);
                    q.add(new Pair<>(n, path));
                }
            }
        }

//        System.out.println("print paths:" + paths.size());
        for (List<Item> p: paths) {
//            System.out.println(String.format("size:%d. path:%s", p.size(), p));
        }

        // remove transitive. retain the longest path. brute force only
        List<Pair<Item, Item>> newR = new ArrayList<>(R);
        for (List<Item> p: paths) {
            if (p.size() > 2) {
                Pair<Item, Item> edge = new Pair<>(p.get(0), p.get(p.size() - 1));
                if (newR.remove(edge)) {
                    adj.get(edge.getKey()).remove(edge.getValue());
                }
            }
        }
//        System.out.println(newR);

        // build reverse adj
        reverseAdj = new HashMap<>();
        for (Map.Entry<Item, Set<Item>> e: adj.entrySet()) {
            Item src = e.getKey();
            for (Item dst: e.getValue()) {
                Pair<Item, Item> edge = new Pair<>(dst, src);
                reverseAdj.computeIfAbsent(dst, key -> new HashSet<>());
                reverseAdj.get(dst).add(src);
            }
        }
//        Map<Pair<Item, Item>, List<List<Item>>> map = new HashMap<>();
//        for (List<Item> p: paths) {
//            Pair<Item, Item> key = new Pair<>(p.get(0), p.get(p.size() - 1));
//            map.computeIfAbsent(key, k -> new ArrayList<>());
//            map.get(key).add(p);
//        }
//        System.out.println(map);
    }

    public Set<Item> search(Item start) {
        Set<Item> ret = new HashSet<>();
        for (Item d: dst) {
            if (isSubset(start, d)) {
                // dfs
                Queue<Item> q = new LinkedList<>();
                q.add(d);
                while (!q.isEmpty()) {
                    Item cur = q.poll();
                    if (isSubset(start, cur)) {
                        ret.add(cur);
                        q.addAll(reverseAdj.getOrDefault(cur, new HashSet<>()));
                    }
                }
            }
        }
        return ret;
    }

    public static Boolean isSubset(Item it1, Item it2) {
        for (String data: it1.data) {
            if (!it2.data.contains(data)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for(Item it: items) {
            sb.append(String.format("index:%d,%s\n", i++, it));
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        LatticeIndex index = new LatticeIndex();
        Set<String> data1 = new HashSet<String>(){{
            add("A");
        }};
        Set<String> data2 = new HashSet<String>(){{
            add("B");
        }};
        Set<String> data3 = new HashSet<String>(){{
            add("D");
        }};
        Set<String> data4 = new HashSet<String>(){{
            add("A");
            add("B");
        }};
        Set<String> data5 = new HashSet<String>(){{
            add("B");
            add("E");
        }};
        Set<String> data6 = new HashSet<String>(){{
            add("A");
            add("B");
            add("C");
        }};
        Set<String> data7 = new HashSet<String>(){{
            add("A");
            add("B");
            add("F");
        }};
        Set<String> data8 = new HashSet<String>(){{
            add("B");
            add("C");
            add("D");
            add("E");
        }};
        index.add(new Item(data1));
        index.add(new Item(data1));
        index.add(new Item(data2));
        index.add(new Item(data3));
        index.add(new Item(data4));
        index.add(new Item(data5));
        index.add(new Item(data6));
        index.add(new Item(data7));
        index.add(new Item(data8));
        index.add(new Item(data1));
        index.add(new Item(data1));

        System.out.println(index);
        System.out.println(index.findR());
        System.out.println(index.init());
        index.deleteTransitive();
        System.out.println(index.search(new Item(data1)));
        System.out.println(index.search(new Item(data2)));
        System.out.println(index.search(new Item(data3)));
    }
}
