package org.zzt.index;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zzt.entity.TableInfo;
import org.zzt.service.SparkService;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class Index {
    @Autowired
    SparkService sparkService;
    public Boolean initialized = false;
    private final LatticeIndex sourceLevel = new LatticeIndex();
    private final LatticeIndex outputLevel = new LatticeIndex();
    private final LatticeRangeIndex predicateIndex = new LatticeRangeIndex();

//     tables to level1
//    private final Map<Set<String>, Item> tables = new HashMap<>();
//     level1 to level2
    private final Map<Item, Set<Item>> pointers1 = new HashMap<>();
    // level2 to level3
    private final Map<Item, Set<RangeItem>> pointers2 = new HashMap<>();

    // level1 mapping
    private final Map<String, Item> tableToLevel1 = new HashMap<>();
    private final Map<Item, Set<String>> Level1ToTables = new HashMap<>();
    // level2 mapping
    private final Map<String, Item> tableToLevel2 = new HashMap<>();
    private final Map<Item, Set<String>> Level2ToTables = new HashMap<>();
    // level3 mapping
    private final Map<String, RangeItem> tableToLevel3 = new HashMap<>();
    private final Map<RangeItem, Set<String>> Level3ToTables = new HashMap<>();

    private final Map<String, Map<String, Set<String>>> tableGraphs = new HashMap<>();

    private Map<String, String> primaryKey2Table = new HashMap<String, String>(){{
        put("L_ORDERKEY", "spark_catalog.default.lineitem");
        put("O_ORDERKEY", "spark_catalog.default.orders");
        put("P_PARTKEY", "spark_catalog.default.part");
        put("PS_PARTKEY", "spark_catalog.default.partsupp");
        put("C_CUSTKEY", "spark_catalog.default.customer");
        put("N_NATIONKEY", "spark_catalog.default.nation");
        put("S_SUPPKEY", "spark_catalog.default.supplier");
        put("R_REGIONKEY", "spark_catalog.default.region");
    }};
    private Set<String> primaryKeyset = primaryKey2Table.keySet();

    private Map<String, String> foreignKey2Table = new HashMap<String, String>(){{
        put("L_PARTKEY", "spark_catalog.default.lineitem");
        put("L_SUPPKEY", "spark_catalog.default.lineitem");
        put("O_CUSTKEY", "spark_catalog.default.orders");
        put("PS_SUPPKEY", "spark_catalog.default.partsupp");
        put("C_NATIONKEY", "spark_catalog.default.customer");
        put("N_REGIONKEY", "spark_catalog.default.nation");
        put("S_NATIONKEY", "spark_catalog.default.supplier");
    }};
    private Set<String> foreignKeyset = foreignKey2Table.keySet();
    public void initIndex(List<TableInfo> tableInfos) {
        // load all pre_tables info
//        List<TableInfo> tableInfos = new ArrayList<>(); // load from hive metastore
        for (TableInfo info: tableInfos) {
            String name = info.name;
            Item level1 = info.level1, level2 = info.level2;
            RangeItem level3 = info.level3;

            Level1ToTables.putIfAbsent(level1, new HashSet<>());
            Level2ToTables.putIfAbsent(level2, new HashSet<>());
            Level3ToTables.putIfAbsent(level3, new HashSet<>());

            tableToLevel1.put(name, level1);
            Level1ToTables.get(level1).add(name);
            tableToLevel2.put(name, level2);
            Level2ToTables.get(level2).add(name);
            tableToLevel3.put(name, level3);
            Level3ToTables.get(level3).add(name);

            Map<String, Set<String>> g = buildGraph(info);
            reduce(g);
            tableGraphs.put(name, g);

            sourceLevel.add(level1);
            outputLevel.add(level2);
            predicateIndex.add(level3);
        }
        sourceLevel.init();
        sourceLevel.deleteTransitive();
        outputLevel.init();
        outputLevel.deleteTransitive();
        predicateIndex.init();
        predicateIndex.deleteTransitive();

        // connect levels
        for (Map.Entry<Item, Set<String>> e: Level1ToTables.entrySet()) {
            Item item = e.getKey();
            Set<String> relatedTables = e.getValue();
//                tables.put(relatedTables, item);
            // connect 1 and 2
            pointers1.putIfAbsent(item, new HashSet<>());
            for (String table: relatedTables) {
                Item next = tableToLevel2.get(table);
                pointers1.get(item).add(next);
            }
        }

        for (Map.Entry<Item, Set<String>> e: Level2ToTables.entrySet()) {
            Item item = e.getKey();
            Set<String> relatedTables = e.getValue();
            // connect 2 and 3
            pointers2.putIfAbsent(item, new HashSet<>());
            for (String table: relatedTables) {
                RangeItem next = tableToLevel3.get(table);
                pointers2.get(item).add(next);
            }
        }
        log.info("init index finished");
        initialized = true;
    }

    public Set<String> search(String sql) {
        // spark.sql(...)
        TableInfo tableInfo = sparkService.getTableInfo(sql);
        // get tables
        Item level1 = tableInfo.level1;
        // get outputs
        Item level2 = tableInfo.level2;
        // get predicates. TODO
        RangeItem level3 = tableInfo.level3;

        // search level1
        Set<Item> superSet1 = sourceLevel.search(level1);
        Set<Item> next1 = new HashSet<>();
        Set<String> candidate1 = new HashSet<>();
        superSet1.forEach(item -> {
            if (item.equals(level1)) {
                candidate1.addAll(Level1ToTables.get(item));
                next1.addAll(pointers1.get(item));
            } else {
                // foreign join.
                Map<String, Set<String>> g = buildGraph(tableInfo);
                reduce(g);

                Set<String> tables = Level1ToTables.get(item);
                for (String table: tables) {
                    Map<String, Set<String>> graph = tableGraphs.get(table);
                    reduce(graph);
                    if (g.equals(graph)) {
                        candidate1.add(table);
                        next1.addAll(pointers1.get(item));
                    }
                }
            }
        });
        // search level2
        Set<Item> superSet2 = outputLevel.search(level2);
        Set<RangeItem> next2 = new HashSet<>();
        Set<String> candidate2 = new HashSet<>();
        superSet2.stream().filter(next1::contains).forEach(item -> {
            next2.addAll(pointers2.get(item));
            candidate2.addAll(Level2ToTables.get(item));
        });
        // search level3
        Set<RangeItem> superSet3 = predicateIndex.search(level3);
        Set<String> candidate = new HashSet<>();
        superSet3.stream().filter(next2::contains).forEach(item -> {
            candidate.addAll(Level3ToTables.get(item));
        });

        candidate.retainAll(candidate1);
        candidate.retainAll(candidate2);
        return candidate;
    }

    Boolean equalJoinGraph(Item a, Item b) {
        return true;
    }

    Map<String, Set<String>> buildGraph(TableInfo tableInfo) {
        Map<String, Set<String>> ret = new HashMap<>();
        tableInfo.level1.data.forEach(table -> {
            ret.putIfAbsent(table, new HashSet<>());
        });
        Set<TableInfo.Edge> joins = tableInfo.equalJoins;
        joins.forEach(f -> {
            String key1 = f.node1, key2 = f.node2;
            if (primaryKeyset.contains(key1) && foreignKeyset.contains(key2)) {
                String table1 = primaryKey2Table.get(key1);
                ret.get(table1).add(foreignKey2Table.get(key2));
            } else if (primaryKeyset.contains(key2) && foreignKeyset.contains(key1)) {
                String table2 = primaryKey2Table.get(key2);
                ret.get(table2).add(foreignKey2Table.get(key1));
            } else {
                log.error("invalid primary key:{}, {}", key1, key2);
            }
        });
        return ret;
    }

    static void reduce(Map<String, Set<String>> g) {
        Boolean stop = false;
        while (!stop) {
            stop = true;
            for(Iterator it = g.keySet().iterator(); it.hasNext(); ) {
                String key = (String) it.next();
                // enumerate incoming edge
                int count = 0;
                for (Set<String> s: g.values()) {
                    if (s.contains(key)) {
                        count++;
                    }
                }
                // eliminate
                if (g.get(key).size() == 0 && count == 1) {
                    // remove relative edge
                    for (String table: g.keySet()) {
                        g.get(table).remove(key);
                    }
                    // remove node
                    it.remove();
                    stop = false;
                }
            }
        }
    }

    public static void main(String[] args) {
        Map<String, Set<String>> g1 = new HashMap<>(), g2 = new HashMap<>();
        g1.put("t1", new HashSet<>(Collections.singletonList("t2")));
        g1.put("t2", new HashSet<>(Collections.singletonList("t3")));
        g1.put("t3", new HashSet<>());

        g2.put("t1", new HashSet<>());

        reduce(g1);
        reduce(g2);

        System.out.println(g1.equals(g2));
    }
}
