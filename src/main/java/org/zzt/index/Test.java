package org.zzt.index;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

public class Test {
    private static final List<String> tables = new ArrayList<String>(){{
        add("T0");
        add("T1");
        add("T2");
        add("T3");
        add("T4");
        add("T5");
        add("T6");
        add("T7");
        add("T8");
        add("T9");
    }};
    private static Map<String, Item> sqlToTables;
    private static Map<Item, Set<String>> tableToSql;
    private static List<Item> generateData(int size) {
        List<Item> ret = new ArrayList<>();
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
        sqlToTables = new HashMap<>();
        tableToSql = new HashMap<>();
        String sqlBase = "sql%d";
        int index = 0;
        for (int i = 0; i < size; ++i) {
            Item item = new Item(data1);
            String name = String.format(sqlBase, index++);
            ret.add(item);
            sqlToTables.put(name, item);
            tableToSql.computeIfAbsent(item, key -> new HashSet<>());
            tableToSql.get(item).add(name);

            item = new Item(data2);
            name = String.format(sqlBase, index++);
            ret.add(item);
            sqlToTables.put(name, item);
            tableToSql.computeIfAbsent(item, key -> new HashSet<>());
            tableToSql.get(item).add(name);

            item = new Item(data3);
            name = String.format(sqlBase, index++);
            ret.add(item);
            sqlToTables.put(name, item);
            tableToSql.computeIfAbsent(item, key -> new HashSet<>());
            tableToSql.get(item).add(name);

            item = new Item(data4);
            name = String.format(sqlBase, index++);
            ret.add(item);
            sqlToTables.put(name, item);
            tableToSql.computeIfAbsent(item, key -> new HashSet<>());
            tableToSql.get(item).add(name);

            item = new Item(data5);
            name = String.format(sqlBase, index++);
            ret.add(item);
            sqlToTables.put(name, item);
            tableToSql.computeIfAbsent(item, key -> new HashSet<>());
            tableToSql.get(item).add(name);

            item = new Item(data6);
            name = String.format(sqlBase, index++);
            ret.add(item);
            sqlToTables.put(name, item);
            tableToSql.computeIfAbsent(item, key -> new HashSet<>());
            tableToSql.get(item).add(name);

            item = new Item(data7);
            name = String.format(sqlBase, index++);
            ret.add(item);
            sqlToTables.put(name, item);
            tableToSql.computeIfAbsent(item, key -> new HashSet<>());
            tableToSql.get(item).add(name);

            item = new Item(data8);
            name = String.format(sqlBase, index++);
            ret.add(item);
            sqlToTables.put(name, item);
            tableToSql.computeIfAbsent(item, key -> new HashSet<>());
            tableToSql.get(item).add(name);
        }
//        Random random = new Random();
//        // each item size
//        for (int i = 1; i <= 6; ++i) {
//            for (int c = 0; c < 10000; ++c) {
//                Set<String> data = new HashSet<>();
//                for (int j = 0; j < i; ++j) {
//                    data.add(tables.get(random.nextInt(100) % 10));
//                }
//                ret.add(new Item(data));
//            }
//        }
        return ret;
    }

    public static long base(List<Item> src, Map<String, Item> sqlToTables) {
        Set<Item> res = new HashSet<>();
        List<String> tables = new ArrayList<>();
        Long start = System.currentTimeMillis();
        for (Item s: src) {
            for(Map.Entry<String, Item> e: sqlToTables.entrySet()) {
                if (LatticeIndex.isSubset(s, e.getValue())) {
                    res.add(e.getValue());
                    tables.add(e.getKey());
                }
            }
        }
        Long end = System.currentTimeMillis();
//        System.out.println(tables.size());
//        System.out.println(res);
//        System.out.println(start);
//        System.out.println(end);
//        System.out.println(end - start);
        return end - start;
    }

    public static long test(List<Item> src, Map<String, Item> sqlToTables) {
        Set<Item> res = new HashSet<>();
        List<String> tables = new ArrayList<>();

        List<Item> data = new ArrayList<>(sqlToTables.values());
        LatticeIndex index = new LatticeIndex();
        data.forEach(index::add);
        index.init();
        index.deleteTransitive();

        Long start = System.currentTimeMillis();
        for (Item s: src) {
            Set<Item> tmp = index.search(s);
            res.addAll(tmp);
            tmp.forEach(f -> {
                tables.addAll(tableToSql.get(f));
            });
        }
        Long end = System.currentTimeMillis();
//        System.out.println(tables.size());
//        System.out.println(res);
//        System.out.println(start);
//        System.out.println(end);
//        System.out.println(end - start);
        return end - start;
    }

    public static void main(String[] args) throws Exception {
//        List<Item> data = generateData(1);
        List<Item> src = new ArrayList<Item>(){{
            add(new Item(new HashSet<String>(){{
                add("A");
                add("B");
            }}));
            add(new Item(new HashSet<String>(){{
                add("B");
                add("E");
            }}));
            add(new Item(new HashSet<String>(){{
                add("A");
            }}));
        }};
//        base(src, sqlToTables);
//        test(src, sqlToTables);

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(new File("res.csv")));
        for (int i = 100; i <= 10000; i = i + 100) {
            generateData(i);
            System.out.println(i);
            bufferedWriter.write(String.format("base:%d, test:%d\n", base(src, sqlToTables), test(src, sqlToTables)));
        }
        bufferedWriter.close();
    }
}
