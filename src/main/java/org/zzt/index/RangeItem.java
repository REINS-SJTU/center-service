package org.zzt.index;

import java.util.*;

public class RangeItem {
    public Map<Set<String>, Range> attr;
//    public Set<String> allAttr = new HashSet<>();

    public RangeItem() {
        this.attr = new HashMap<>();
    }

    // equal_class -> range
    public RangeItem(Map<Set<String>, Range> items) {
        this.attr = items;
//        items.keySet().forEach(set -> {
//            allAttr.addAll(set);
//        });
    }

    void merge(String k1, String k2) {
        Set<String> a = new HashSet<String>(){{ add(k1);}}, b = new HashSet<String>(){{ add(k2);}};

        Range r1 = attr.get(a), r2 = attr.get(b);
        if (r1 == null || r2 == null) {
            return;
        }
        attr.remove(a);
        attr.remove(b);
        Range range = r1.merge(r2);
        Set<String> classes = new HashSet<String>(){{
            add(k1);
            add(k2);
        }};
        attr.put(classes, range);
    }

    public static class Range {
        public Object lowerBound;
        public Object upperBound;
        public boolean includeLower;
        public boolean includeUpper;

        public Range(Range r) {
            lowerBound = r.lowerBound;
            upperBound = r.upperBound;
            includeLower = r.includeLower;
            includeUpper = r.includeUpper;
        }

        public Range(Object lower, Object upper, boolean includeLower, boolean includeUpper) {
//            this.attribute = attr;
            this.lowerBound = lower;
            this.upperBound = upper;
            this.includeLower = includeLower;
            this.includeUpper = includeUpper;
        }

        public Boolean isSubsetOf(Range other) {
            if (this.lowerBound == null && other.lowerBound != null){
                return false;
            }
            if (this.upperBound == null && other.upperBound != null) {
                return false;
            }
            if (this.lowerBound != null && other.lowerBound != null) {
                if (GT(this.lowerBound, other.lowerBound)) {
                    return true;
                }
                if (this.lowerBound.equals(other.lowerBound)) {
                    if (this.includeLower && !other.includeLower) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
            if (this.upperBound != null && other.upperBound != null) {
                if (GT(other.upperBound, this.upperBound)) {
                    return true;
                }
                if (this.upperBound.equals(other.upperBound)) {
                    if (this.includeUpper && !other.includeUpper) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
            return false;
        }

        public Boolean GT(Object o1, Object o2) {
            if (o1 instanceof Long && o2 instanceof Long) {
                return ((Long) o1) > ((Long) o2);
            }
            if (o1 instanceof String && o2 instanceof String) {
                String a = (String) o1, b = (String) o2;
                int ret = a.compareTo(b);
                return ret > 0;
            }
            return false;
        }

        public Range merge(Range other) {
            if (other.lowerBound != null) {
                if (this.lowerBound == null) {
                    this.lowerBound = other.lowerBound;
                    this.includeLower = other.includeLower;
                } else {
                    // merge
                    if (GT(other.lowerBound, this.upperBound)) {
                        this.lowerBound = other.lowerBound;
                        this.includeLower = other.includeLower;
                    } else if (this.lowerBound.equals(other.lowerBound)) {
                        this.includeLower = (this.includeLower || other.includeLower);
                    }
                }
            }
            if (other.upperBound != null) {
                if (this.upperBound == null) {
                    this.upperBound = other.upperBound;
                    this.includeUpper = other.includeUpper;
                } else {
                    // merge
                    if (GT(this.upperBound, other.upperBound)) {
                        this.upperBound = other.upperBound;
                        this.includeUpper = other.includeUpper;
                    } else if (this.upperBound.equals(other.upperBound)) {
                        this.includeUpper = (this.includeUpper || other.includeUpper);
                    }
                }
            }
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Range) {
                Range r = (Range) o;
                return this.includeLower == r.includeLower
                        && this.includeUpper == r.includeUpper
                        && this.lowerBound.equals(r.lowerBound)
                        && this.upperBound.equals(r.upperBound);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return 7 * upperBound.hashCode() + 11 * lowerBound.hashCode();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RangeItem) {
            RangeItem b = (RangeItem) o;
            Map<Set<String>, Range> data1 = this.attr, data2 = b.attr;
            Set<Set<String>> k1 = data1.keySet(), k2 = data2.keySet();
            if (k1.size() != k2.size() || !k1.containsAll(k2)) {
                return false;
            }
            for (Map.Entry<Set<String>, Range> e: data1.entrySet()) {
                if (!e.getValue().equals(data2.get(e.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return attr.hashCode();
    }

    public static void main(String[] args) throws Exception {
        RangeItem r1 = new RangeItem();
        RangeItem r2 = new RangeItem();
        System.out.println(r1.equals(r2));

        Set<Set<String>> s1 = new HashSet<>(), s2 = new HashSet<>();
        System.out.println(s1.equals(s2));
        System.out.println(s1.containsAll(s2));
    }
}
