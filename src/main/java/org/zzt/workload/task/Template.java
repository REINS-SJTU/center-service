package org.zzt.workload.task;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

public class Template {
    public String sqlToMV(String sql) {
        String targetMv = sql.replaceAll(pattern, "''");
        String ret = tempToMv.getOrDefault(targetMv, "");
//        System.out.println(targetMv);
//        System.out.println(temp1);
        return ret;
    }

    public static Map<Integer, String> index2Template = new HashMap<Integer, String>(){{
        put(1, temp1);
        put(2, temp2);
        put(3, temp3);
        put(4, temp4);
        put(5, temp5);
        put(6, temp6);
        put(7, temp7);
        put(8, temp8);
        put(9, temp9);
        put(10, temp10);
        put(11, temp11);
        put(12, temp12);
    }};

    public String pattern = "'[\\S\\s]*'";
    public Map<String, String> tempToMv = new HashMap<String, String>(){{
        put(temp1.replaceAll(pattern, "''"), mv1);
        put(temp2.replaceAll(pattern, "''"), mv2);
        put(temp3.replaceAll(pattern, "''"), mv3);
        put(temp4.replaceAll(pattern, "''"), mv4);
        put(temp5.replaceAll(pattern, "''"), mv5);
        put(temp6.replaceAll(pattern, "''"), mv6);
        put(temp7.replaceAll(pattern, "''"), mv7);
        put(temp8.replaceAll(pattern, "''"), mv8);
        put(temp9.replaceAll(pattern, "''"), mv9);
        put(temp10.replaceAll(pattern, "''"), mv10);
        put(temp11.replaceAll(pattern, "''"), mv11);
        put(temp12.replaceAll(pattern, "''"), mv12);
    }};
    public Map<String, Integer> mvToIndex = new HashMap<String, Integer>(){{
        put(mv1, 1);
        put(mv2, 2);
        put(mv3, 3);
        put(mv4, 4);
        put(mv5, 5);
        put(mv6, 6);
        put(mv7, 7);
        put(mv8, 8);
        put(mv9, 9);
        put(mv10, 10);
        put(mv11, 11);
        put(mv12, 12);
    }};
    public Map<String, String> mvNameToSql = new HashMap<String, String>() {{
        put("mv1", mv1);
        put("mv2", mv2);
        put("mv3", mv3);
        put("mv4", mv4);
        put("mv5", mv5);
        put("mv6", mv6);
        put("mv9", mv7);
        put("mv10", mv8);
        put("mv13", mv9);
        put("mv16", mv10);
        put("mv17", mv11);
        put("mv18", mv12);
    }};
    public Map<String, String> mvSqlToName = new HashMap<String, String>() {{
        put(mv1, "mv1");
        put(mv2, "mv2");
        put(mv3, "mv3");
        put(mv4, "mv4");
        put(mv5, "mv5");
        put(mv6, "mv6");
        put(mv7, "mv9");
        put(mv8, "mv10");
        put(mv9, "mv13");
        put(mv10, "mv16");
        put(mv11, "mv17");
        put(mv12, "mv18");
    }};
    public static Map<Integer, String> index2MV = new HashMap<Integer, String>(){{
        put(1, "mv1");
        put(2, "mv2");
        put(3, "mv3");
        put(4, "mv4");
        put(5, "mv5");
        put(6, "mv6");
        put(7, "mv9");
        put(8, "mv10");
        put(9, "mv13");
        put(10, "mv16");
        put(11, "mv17");
        put(12, "mv18");
    }};

    public static String temp1 = // query 1
            "select L_RETURNFLAG, L_LINESTATUS, " +
            "       sum(L_QUANTITY) as qty, " +
            "       sum(L_EXTENDEDPRICE) as sum_base_price, " +
            "       count(*) as count_order " +
            "from lineitem l " +
            "where L_LINESTATUS <> '%s' " +
            "group by L_RETURNFLAG, L_LINESTATUS";
    public static String mv1 =
            "select L_RETURNFLAG, L_LINESTATUS, " +
            "       sum(L_QUANTITY) as qty, " +
            "       sum(L_EXTENDEDPRICE) as sum_base_price, " +
            "       count(*) as count_order " +
            "from lineitem l " +
            "group by L_RETURNFLAG, L_LINESTATUS";
    public static String temp2 = // query 2
            "select S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT, R_NAME, PS_SUPPLYCOST, P_SIZE, P_TYPE " +
            "from PART, SUPPLIER, PARTSUPP, NATION, REGION " +
            "where P_PARTKEY = PS_PARTKEY " +
            "      and S_SUPPKEY = PS_SUPPKEY " +
            "      and S_NATIONKEY = N_NATIONKEY " +
            "      and N_REGIONKEY = R_REGIONKEY " +
            "      and R_NAME = '%s' " +
            "      and PS_SUPPLYCOST < '%s' " +
            "      and P_SIZE = '%s' " +
            "      and P_TYPE like '%%%s%%'";
    public static String mv2 =
            "select S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT, R_NAME, PS_SUPPLYCOST, P_SIZE, P_TYPE " +
            "from PART, SUPPLIER, PARTSUPP, NATION, REGION " +
            "where P_PARTKEY = PS_PARTKEY " +
            "      and S_SUPPKEY = PS_SUPPKEY " +
            "      and S_NATIONKEY = N_NATIONKEY " +
            "      and N_REGIONKEY = R_REGIONKEY ";
    public static String temp3 = // query 3
            "select  L_ORDERKEY, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue, O_ORDERDATE, O_SHIPPRIORITY " +
            "from CUSTOMER, ORDERS, LINEITEM " +
            "where C_MKTSEGMENT = 'MACHINERY' " +
            "     and C_CUSTKEY = O_CUSTKEY " +
            "     and L_ORDERKEY = O_ORDERKEY " +
            "     and O_ORDERDATE < '%s' " +
            "group by L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY";
    public static String mv3 =
            "select  L_ORDERKEY, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue, O_ORDERDATE, O_SHIPPRIORITY " +
            "from CUSTOMER, ORDERS, LINEITEM " +
            "where C_MKTSEGMENT = 'MACHINERY' " +
            "     and C_CUSTKEY = O_CUSTKEY " +
            "     and L_ORDERKEY = O_ORDERKEY " +
            "group by L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY";
    public static String temp4 = // query 4
            "select O_ORDERPRIORITY, count(*) as order_count " +
            "from ORDERS " +
            "where O_ORDERPRIORITY = '%s' " +
            "group by O_ORDERPRIORITY";
    public static String mv4 =
            "select O_ORDERPRIORITY, count(*) as order_count " +
            "from ORDERS " +
            "group by O_ORDERPRIORITY";
    public static String temp5 = // query 5
            "select N_NAME, R_NAME, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue " +
            "from CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION " +
            "where C_CUSTKEY = O_CUSTKEY " +
            "      and L_ORDERKEY = O_ORDERKEY " +
            "      and L_SUPPKEY = S_SUPPKEY " +
            "      and C_NATIONKEY = S_NATIONKEY " +
            "      and S_NATIONKEY = N_NATIONKEY " +
            "      and N_REGIONKEY = R_REGIONKEY " +
            "      and R_NAME = '%s' " +
            "group by N_NAME, R_NAME";
    public static String mv5 =
            "select N_NAME, R_NAME, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue " +
            "from CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION " +
            "where C_CUSTKEY = O_CUSTKEY " +
            "      and L_ORDERKEY = O_ORDERKEY " +
            "      and L_SUPPKEY = S_SUPPKEY " +
            "      and C_NATIONKEY = S_NATIONKEY " +
            "      and S_NATIONKEY = N_NATIONKEY " +
            "      and N_REGIONKEY = R_REGIONKEY " +
            "group by N_NAME, R_NAME";
    public static String temp6 = // query 6
            "select sum(L_EXTENDEDPRICE * L_DISCOUNT) as revenue " +
            "from LINEITEM";
    public static String mv6 =
            "select sum(L_EXTENDEDPRICE * L_DISCOUNT) as revenue " +
                    "from LINEITEM";
    public static String temp7 = // query 9
            "select N_NAME, P_NAME, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as amount " +
            "from PART, SUPPLIER, LINEITEM, PARTSUPP, ORDERS, NATION " +
            "where S_SUPPKEY = L_SUPPKEY " +
            "      and PS_SUPPKEY = L_SUPPKEY " +
            "      and PS_PARTKEY = L_PARTKEY " +
            "      and P_PARTKEY = L_PARTKEY " +
            "      and O_ORDERKEY = L_ORDERKEY " +
            "      and S_NATIONKEY = N_NATIONKEY " +
            "      and P_NAME like '%%%s%%' " +
            "group by N_NAME, P_NAME";
    public static String mv7 =
            "select N_NAME, P_NAME, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as amount " +
            "from PART, SUPPLIER, LINEITEM, PARTSUPP, ORDERS, NATION " +
            "where S_SUPPKEY = L_SUPPKEY " +
            "      and PS_SUPPKEY = L_SUPPKEY " +
            "      and PS_PARTKEY = L_PARTKEY " +
            "      and P_PARTKEY = L_PARTKEY " +
            "      and O_ORDERKEY = L_ORDERKEY " +
            "      and S_NATIONKEY = N_NATIONKEY " +
            "group by N_NAME, P_NAME";
    public static String temp8 = // query 10
            "select C_CUSTKEY, C_NAME, " +
            "       sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue, " +
            "       C_ACCTBAL, N_NAME, C_ADDRESS, C_PHONE, C_COMMENT, L_RETURNFLAG " +
            "from CUSTOMER, ORDERS, LINEITEM, NATION " +
            "where C_CUSTKEY = O_CUSTKEY " +
            "      and L_ORDERKEY = O_ORDERKEY " +
            "      and C_NATIONKEY = N_NATIONKEY " +
            "      and L_RETURNFLAG = '%s' " +
            "group by C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, N_NAME, C_ADDRESS, C_COMMENT, L_RETURNFLAG";
    public static String mv8 =
            "select C_CUSTKEY, C_NAME, " +
            "       sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue, " +
            "       C_ACCTBAL, N_NAME, C_ADDRESS, C_PHONE, C_COMMENT, L_RETURNFLAG " +
            "from CUSTOMER, ORDERS, LINEITEM, NATION " +
            "where C_CUSTKEY = O_CUSTKEY " +
            "      and L_ORDERKEY = O_ORDERKEY " +
            "      and C_NATIONKEY = N_NATIONKEY " +
            "group by C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, N_NAME, C_ADDRESS, C_COMMENT, L_RETURNFLAG";
    public static String temp9 = // query 13
            "select OUT_COUNT, count(*) as custdist " +
            "from (select C_CUSTKEY, count(*) as C_COUNT " +
            "      from CUSTOMER left outer join ORDERS " +
            "      on C_CUSTKEY = O_CUSTKEY " +
            "      group by C_CUSTKEY) as c_orders (C_CUSTKEY, OUT_COUNT) " +
            "group by OUT_COUNT";
    public static String mv9 =
            "select C_CUSTKEY, count(*) as C_COUNT " +
            "from CUSTOMER left outer join ORDERS " +
            "     on C_CUSTKEY = O_CUSTKEY " +
            "     group by C_CUSTKEY";
    public static String temp10 = // query 16
            "select P_BRAND, P_TYPE, P_SIZE, " +
            "       count(*) as supplier_cnt " +
            "from PARTSUPP, PART " +
            "where P_PARTKEY = PS_PARTKEY " +
            "      and P_BRAND <> '%s' " +
            "      and P_SIZE = '%s' " +
            "      and P_TYPE not like '%s%%' " +
            "group by P_BRAND, P_TYPE, P_SIZE";
    public static String mv10 =
            "select P_BRAND, P_TYPE, P_SIZE, " +
            "       count(*) as supplier_cnt " +
            "from PARTSUPP, PART " +
            "where P_PARTKEY = PS_PARTKEY " +
            "group by P_BRAND, P_TYPE, P_SIZE";
    public static String temp11 = // query 17
            "select P_BRAND, P_CONTAINER, sum(L_EXTENDEDPRICE / 7.0) as avg_year " +
            "from LINEITEM, PART " +
            "where P_PARTKEY = L_PARTKEY " +
            "      and P_BRAND = '%s' " +
            "      and P_CONTAINER = '%s' " +
            "group by P_BRAND, P_CONTAINER";
    public static String mv11 =
            "select P_BRAND, P_CONTAINER, sum(L_EXTENDEDPRICE / 7.0) as avg_year " +
            "from LINEITEM, PART " +
            "where P_PARTKEY = L_PARTKEY " +
            "group by P_BRAND, P_CONTAINER";
    public static String temp12 = // query 18
            "select C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, sum(L_QUANTITY) as quantity " +
            "from CUSTOMER, ORDERS, LINEITEM " +
            "where O_ORDERKEY = '%s' " +
            "      and C_CUSTKEY = O_CUSTKEY " +
            "      and O_ORDERKEY = L_ORDERKEY " +
            "group by C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE";
    public static String mv12 =
            "select C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, sum(L_QUANTITY) as quantity " +
            "from CUSTOMER, ORDERS, LINEITEM " +
            "where C_CUSTKEY = O_CUSTKEY " +
            "      and O_ORDERKEY = L_ORDERKEY " +
            "group by C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE";
    public static String build1(String a) {
        return String.format(temp1, a);
    }
    public static String build2(String a, Double b, Integer c, String d) {
        return String.format(temp2, a, String.format("%.1f", b), c, d);
    }
    public static String build3(String a) {
        return String.format(temp3, a);
    }
    public static String build4(String a) {
        return String.format(temp4, a);
    }
    public static String build5(String a){
        return String.format(temp5, a);
    }
    public static String build6(){
        return temp6;
    }
    public static String build7(String a){
        return String.format(temp7, a);
    }
    public static String build8(String a){
        return String.format(temp8, a);
    }
    public static String build9(){
        return temp9;
    }
    public static String build10(String a, Integer b, String c){
        return String.format(temp10, a, b, c);
    }
    public static String build11(String a, String b){
        return String.format(temp11, a, b);
    }
    public static String build12(Integer a){
        return String.format(temp12, a);
    }
    public static Random random = new Random();
    public static String random1() {
        List<String> candidate = new ArrayList<String>(){{
            add("O");
            add("F");
        }};
        Collections.shuffle(candidate);
        return build1(candidate.get(0));
    }
    public static String random2() {
        List<String> name = Arrays.asList("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST");
        List<String> type = Arrays.asList("LARGE", "MEDIUM", "SMALL", "BRUSHED", "NICKEL", "POLISHED", "ECONOMY", "PLATED");
        Collections.shuffle(name);
        Collections.shuffle(type);
        return build2(name.get(0), random.nextFloat() * 999 + 1.0, random.nextInt(50) + 1, type.get(0));
    }
    public static String random3() {
//        List<String> mkt = Arrays.asList("MACHINERY", "AUTOMOBILE", "BUILDING", "HOUSEHOLD", "FURNITURE");
        String date = "1995-03-06";
//        Collections.shuffle(mkt);
        return build3(date);
    }
    public static String random4() {
        List<String> priority = Arrays.asList("1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW");
        Collections.shuffle(priority);
        return build4(priority.get(0));
    }
    public static String random5() {
        List<String> name = Arrays.asList("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST");
        Collections.shuffle(name);
        return build5(name.get(0));
    }
    public static String random6() {
        return build6();
    }
    public static String random7() {
        List<String> color = Arrays.asList("green", "blue", "royal", "peach", "wheat", "dark", "black", "azure", "navy");
        Collections.shuffle(color);
        return build7(color.get(0));
    }
    public static String random8() {
        List<String> flag = Arrays.asList("A", "N", "R");
        Collections.shuffle(flag);
        return build8(flag.get(0));
    }
    public static String random9() {
        return build9();
    }
    public static String random10() {
        List<String> brand = new ArrayList<>();
        for(int i = 11; i <= 55; ++i) {
            brand.add(String.valueOf(i));
        }
        List<String> type = Arrays.asList("LARGE", "MEDIUM", "SMALL", "BRUSHED", "NICKEL", "POLISHED", "ECONOMY", "PLATED");
        Collections.shuffle(brand);
        Collections.shuffle(type);
        return build10("Brand#" + brand.get(0), random.nextInt(50) + 1, type.get(0));
    }
    public static String random11() {
        List<String> brand = new ArrayList<>();
        for(int i = 11; i <= 55; ++i) {
            brand.add(String.valueOf(i));
        }
        List<String> container = Arrays.asList("JUMBO BAG", "JUMBO BOX", " JUMBO CAN", "JUMBO CASE");
        Collections.shuffle(brand);
        Collections.shuffle(container);
        return build11("Brand#" + brand.get(0), container.get(0));
    }
    public static String random12() {
        List<Integer> key = Arrays.asList(1, 2, 3, 4, 5);
        Collections.shuffle(key);
        return build12(key.get(0));
    }
}
