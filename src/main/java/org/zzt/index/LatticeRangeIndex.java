package org.zzt.index;

import org.apache.commons.math3.util.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class LatticeRangeIndex {
    public Set<RangeItem> items;
    public List<Pair<RangeItem, RangeItem>> R;
    public Map<RangeItem, Set<RangeItem>> adj;
    public Map<RangeItem, Set<RangeItem>> reverseAdj;
    private Set<RangeItem> src;
    private Set<RangeItem> dst;

    public LatticeRangeIndex() {
        items = new HashSet<>();
    }

    public void add(RangeItem item) {
        items.add(item);
    }

    public List<Pair<RangeItem, RangeItem>> findR() {
        if (R != null) {
            return R;
        }
        List<Pair<RangeItem, RangeItem>> ret = new ArrayList<>();
        List<RangeItem> list = new ArrayList<>(items);
        int len = list.size();
        for (int i = 0; i < len; ++i) {
            for (int j = i + 1; j < len; ++j) {
                RangeItem it1 = list.get(i), it2 = list.get(j);
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

    public Map<RangeItem, Set<RangeItem>> init() {
        if (adj != null) {
            return adj;
        }
        Map<RangeItem, Set<RangeItem>> ret = new HashMap<>();

        List<Pair<RangeItem, RangeItem>> R = findR();
        for(Pair p: R) {
            RangeItem src = (RangeItem) p.getKey(), dst = (RangeItem) p.getValue();
            ret.computeIfAbsent(src, key -> new HashSet<>());
            ret.get(src).add(dst);
        }
        adj = ret;
        // source nodes
        Set<RangeItem> in = R.stream().map(Pair::getValue).collect(Collectors.toSet());
        src = new HashSet<>(items);
        src.removeAll(in);
        // dst nodes
        dst = new HashSet<>(items);
        dst.removeAll(adj.keySet());
        return ret;
    }

    public void deleteTransitive() {
        Map<RangeItem, Set<RangeItem>> adj = init();
        // find all path
        Set<List<RangeItem>> paths = new HashSet<>();

        // find all path
        for (RangeItem start: src) {
            // dfs
            Queue<Pair<RangeItem, List<RangeItem>>> q = new LinkedList<>();
            q.add(new Pair<>(start, new ArrayList<RangeItem>(){{add(start);}}));
            while (!q.isEmpty()) {
                Pair<RangeItem, List<RangeItem>> cur = q.poll();
                RangeItem node = cur.getKey();
                List<RangeItem> p = cur.getValue();
                paths.add(p);
                Set<RangeItem> next = adj.getOrDefault(node, new HashSet<>());
                for (RangeItem n: next) {
                    List<RangeItem> path = new ArrayList<>(p);
                    path.add(n);
                    q.add(new Pair<>(n, path));
                }
            }
        }

//        System.out.println("print paths:" + paths.size());
        for (List<RangeItem> p: paths) {
//            System.out.println(String.format("size:%d. path:%s", p.size(), p));
        }

        // remove transitive. retain the longest path. brute force only
        List<Pair<RangeItem, RangeItem>> newR = new ArrayList<>(R);
        for (List<RangeItem> p: paths) {
            if (p.size() > 2) {
                Pair<RangeItem, RangeItem> edge = new Pair<>(p.get(0), p.get(p.size() - 1));
                if (newR.remove(edge)) {
                    adj.get(edge.getKey()).remove(edge.getValue());
                }
            }
        }

        // build reverse adj
        reverseAdj = new HashMap<>();
        for (Map.Entry<RangeItem, Set<RangeItem>> e: adj.entrySet()) {
            RangeItem src = e.getKey();
            for (RangeItem dst: e.getValue()) {
                Pair<RangeItem, RangeItem> edge = new Pair<>(dst, src);
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

    public Set<RangeItem> search(RangeItem start) {
        Set<RangeItem> ret = new HashSet<>();
        for (RangeItem d: dst) {
            if (isSubset(start, d)) {
                // dfs
                Queue<RangeItem> q = new LinkedList<>();
                q.add(d);
                while (!q.isEmpty()) {
                    RangeItem cur = q.poll();
                    if (isSubset(start, cur)) {
                        ret.add(cur);
                        q.addAll(reverseAdj.getOrDefault(cur, new HashSet<>()));
                    }
                }
            }
        }
        return ret;
    }

    public static Boolean isSubset(RangeItem it1, RangeItem it2) {
        // 等价类的attr，查询SQL的等价类应当是预计算表的超集，即预计算表的每个等价类都可以在查询SQL中找到超集
        // 并且其对应的值域是查询SQL中的超集
        Set<Set<String>> class1 = it1.attr.keySet(), class2 = it2.attr.keySet();
        for (Set<String> set2 : class2) {
            boolean valid = false;
            for (Set<String> set1 : class1) {
                if (set1.containsAll(set2)) {
                    RangeItem.Range r1 = it1.attr.get(set1), r2 = it2.attr.get(set2);
                    if (r1.isSubsetOf(r2)) {
                        valid = true;
                    }
                }
            }
            if (!valid) {
                return false;
            }
        }
        return true;
    }
}
