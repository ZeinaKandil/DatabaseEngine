package Nenenenene;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class Page implements Serializable {
    Vector<Record> Rows;
    String key;
    String table;
    int N;


    public Page(String keyy, String T, int n) {
        Rows = new Vector<Record>();
        key = keyy;
        table = T;
        N = n;
    }


    public void insertIntoIndex(Hashtable<String, Object> row, Ref recordrefrence, ArrayList<BPTree> BPTrees, ArrayList<RTree> RTrees) throws DBAppException {
        Set<String> keys = row.keySet();
        for (String k : keys) {
            for (int i = 0; i < BPTrees.size(); i++) {
                if (BPTrees.get(i).ColName.equals(k)) {
                    int type = getType(k);
                    if (type == 1)
                        BPTrees.get(i).insert((Integer) row.get(k), recordrefrence);
                    if (type == 2)
                        BPTrees.get(i).insert((String) row.get(k), recordrefrence);
                    if (type == 3)
                        BPTrees.get(i).insert((double) row.get(k), recordrefrence);
                    if (type == 4)
                        BPTrees.get(i).insert((boolean) row.get(k), recordrefrence);
                    if (type == 5)
                        BPTrees.get(i).insert((Date) row.get(k), recordrefrence);
                }
            }
            for (int i = 0; i < RTrees.size(); i++) {
                if (RTrees.get(i).ColName.equals(k)) {
                    RTrees.get(i).insert((DBPolygon) row.get(k), recordrefrence);
                }
            }
        }
    }

    public void deleteFromIndex(Hashtable<String, Object> row, Ref recordrefrence, ArrayList<BPTree> BPTrees, ArrayList<RTree> RTrees) throws DBAppException {
        Set<String> keys = row.keySet();
        for (String k : keys) {
            for (int i = 0; i < BPTrees.size(); i++) {
                if (BPTrees.get(i).ColName.equals(k)) {
                    int type = getType(k);
                    if (type == 1)
                        BPTrees.get(i).deleteSingleRef((Integer) row.get(k), recordrefrence);
                    if (type == 2)
                        BPTrees.get(i).deleteSingleRef((String) row.get(k), recordrefrence);
                    if (type == 3)
                        BPTrees.get(i).deleteSingleRef((double) row.get(k), recordrefrence);
                    if (type == 4)
                        BPTrees.get(i).deleteSingleRef((boolean) row.get(k), recordrefrence);
                    if (type == 5)
                        BPTrees.get(i).deleteSingleRef((Date) row.get(k), recordrefrence);
                }
            }
            for (int i = 0; i < RTrees.size(); i++) {
                if (RTrees.get(i).ColName.equals(k)) {
                    RTrees.get(i).deleteSingleRef((DBPolygon) row.get(k), recordrefrence);
                }
            }
        }
    }

    public void updateIndex(Hashtable<String, Object> row, Ref old, Ref neww, Hashtable<String, Object> h, ArrayList<BPTree> BPTrees, ArrayList<RTree> RTrees) throws DBAppException {
        Set<String> keys = row.keySet();
        for (String k : keys) {
            for (int j = 0; j < BPTrees.size(); j++) {
                if (BPTrees.get(j).ColName.equals(k)) {
                    int type = getType(k);
                    if (type == 1)
                        BPTrees.get(j).updateRef1((Integer) h.get(k), old, neww);
                    if (type == 2)
                        BPTrees.get(j).updateRef1((String) h.get(k), old, neww);
                    if (type == 3)
                        BPTrees.get(j).updateRef1((double) h.get(k), old, neww);
                    if (type == 4)
                        BPTrees.get(j).updateRef1((boolean) h.get(k), old, neww);
                    if (type == 5)
                        BPTrees.get(j).updateRef1((Date) h.get(k), old, neww);


                }
            }
            for (int i = 0; i < RTrees.size(); i++) {
                if (RTrees.get(i).ColName.equals(k)) {
                    RTrees.get(i).updateRef1((DBPolygon) h.get(k), old, neww);
                }
            }
        }

    }

    public int insert(Hashtable<String, Object> row, int index, ArrayList<BPTree> BPTrees, ArrayList<RTree> RTrees) throws DBAppException {
        Object valueOfKey = row.get(key);
        if (Rows.size() == 0) {
            Rows.add(0, new Record(row, new Position(0, table + index)));
            Ref r = new Ref(table + index, 0);
            insertIntoIndex(row, r, BPTrees, RTrees);
            return 0;
        }
        if (Rows.size() == 1) {

            if (Compare(Rows.get(0).row.get(key), valueOfKey, key, table) > 0) {
                Rows.get(0).position.i = 1;
                Ref old = new Ref(table + index, 0);
                Ref neww = new Ref(table + index, 1);
                Hashtable<String, Object> hashtable = Rows.get(0).row;
                updateIndex(row, old, neww, hashtable, BPTrees, RTrees);

                Rows.add(0, new Record(row, new Position(0, table + index)));
                Ref r = new Ref(table + index, 0);
                insertIntoIndex(row, r, BPTrees, RTrees);


                return 0;
            } else {
                Rows.add(1, new Record(row, new Position(1, table + index)));
                Ref r = new Ref(table + index, 1);
                insertIntoIndex(row, r, BPTrees, RTrees);
            }

            return 1;
        }
        int lo = 0;
        int hi = Rows.size() - 1;
        int mid = (hi + lo) / 2;
        int ans = mid;
        while (lo <= hi) {
            mid = (lo + hi) / 2;
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) > 0) {
                hi = mid - 1;
                ans = mid;
            } else if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) < 0) {
                lo = mid + 1;
                ans = mid + 1;
            } else {
                ans = mid;
                break;
            }


        }
        mid = ans;
        int size = Rows.size();
        for (int j = size - 1; j >= mid; j--) {
            Ref old = new Ref(table + index, Rows.get(j).position.i);
            Rows.get(j).position.i = ((Rows.get(j).position.i) + 1);
            Ref neww = new Ref(table + index, Rows.get(j).position.i);
            Hashtable<String, Object> hashtable = Rows.get(j).row;
            updateIndex(row, old, neww, hashtable, BPTrees, RTrees);
        }

        Rows.add(mid, new Record(row, new Position(mid, table + index)));
        Ref r = new Ref(table + index, mid);
        insertIntoIndex(row, r, BPTrees, RTrees);


        return mid;
    }

    public void updateAllIndexesAfterDelete(Hashtable<String, Object> row, int index, ArrayList<BPTree> BPTrees, ArrayList<RTree> RTrees) throws DBAppException {
        for (int i = 0; i < Rows.size(); i++) {
            Ref old = new Ref(table + index, Rows.get(i).position.i);
            Rows.get(i).position.i = i;
            Ref neww = new Ref(table + index, Rows.get(i).position.i);
            Set<String> keys = row.keySet();
            for (String k : keys) {
                for (int j = 0; j < BPTrees.size(); j++) {
                    if (BPTrees.get(j).ColName.equals(k)) {
                        int type = getType(k);
                        if (type == 1)
                            BPTrees.get(j).updateRef1((Integer) Rows.get(i).row.get(k), old, neww);
                        if (type == 2)
                            BPTrees.get(j).updateRef1((String) Rows.get(i).row.get(k), old, neww);
                        if (type == 3)
                            BPTrees.get(j).updateRef1((double) Rows.get(i).row.get(k), old, neww);
                        if (type == 4)
                            BPTrees.get(j).updateRef1((boolean) Rows.get(i).row.get(k), old, neww);
                        if (type == 5) {
                            BPTrees.get(j).updateRef1((Date) Rows.get(i).row.get(k), old, neww);
                        }
                        break;
                    }
                }
                for (int j = 0; j < RTrees.size(); j++) {
                    if (RTrees.get(j).ColName.equals(k)) {
                        RTrees.get(j).updateRef1((DBPolygon) Rows.get(i).row.get(k), old, neww);
                        break;
                    }
                }
            }
        }
    }

    public void updateAllIndexesAfterDelete(int index, ArrayList<BPTree> BPTrees, ArrayList<RTree> RTrees) throws DBAppException {
        if (Rows.size() > 0) return;
        Set<String> keys = Rows.get(0).row.keySet();
        for (int i = 0; i < Rows.size(); i++) {
            Ref old = new Ref(table + index, Rows.get(i).position.i);
            Rows.get(i).position.i = i;
            Ref neww = new Ref(table + index, Rows.get(i).position.i);
            for (String k : keys) {
                for (int j = 0; j < BPTrees.size(); j++) {
                    if (BPTrees.get(j).ColName.equals(k)) {
                        int type = getType(k);
                        if (type == 1)
                            BPTrees.get(j).updateRef1((Integer) Rows.get(i).row.get(k), old, neww);
                        if (type == 2)
                            BPTrees.get(j).updateRef1((String) Rows.get(i).row.get(k), old, neww);
                        if (type == 3)
                            BPTrees.get(j).updateRef1((double) Rows.get(i).row.get(k), old, neww);
                        if (type == 4)
                            BPTrees.get(j).updateRef1((boolean) Rows.get(i).row.get(k), old, neww);
                        if (type == 5) {
                            BPTrees.get(j).updateRef1((Date) Rows.get(i).row.get(k), old, neww);
                        }
                        break;
                    }
                }
                for (int j = 0; j < RTrees.size(); j++) {
                    if (RTrees.get(j).ColName.equals(k)) {
                        RTrees.get(j).updateRef1((DBPolygon) Rows.get(i).row.get(k), old, neww);
                        break;
                    }
                }
            }
        }
    }

    public void deleteAt(Record record, ArrayList<BPTree> BPTrees, ArrayList<RTree> RTrees) throws DBAppException {
        Ref r = new Ref(record.position.pagename, record.position.i);
        for (String k : record.row.keySet()) {
            for (int m = 0; m < BPTrees.size(); m++) {
                if (BPTrees.get(m).ColName.equals(k)) {
                    int type = getType(k);
                    if (type == 1)
                        BPTrees.get(m).deleteSingleRef((Integer) record.row.get(k), r);
                    if (type == 2)
                        BPTrees.get(m).deleteSingleRef((String) record.row.get(k), r);
                    if (type == 3)
                        BPTrees.get(m).deleteSingleRef((double) record.row.get(k), r);
                    if (type == 4)
                        BPTrees.get(m).deleteSingleRef((boolean) record.row.get(k), r);
                    if (type == 5)
                        BPTrees.get(m).deleteSingleRef((Date) record.row.get(k), r);
                    break;
                }
            }
            for (int j = 0; j < RTrees.size(); j++) {
                if (RTrees.get(j).ColName.equals(k)) {
                    RTrees.get(j).deleteSingleRef((DBPolygon) record.row.get(k), r);
                    break;
                }
            }
        }
        Rows.remove(record.position.i);
        int indx = 0;
        for (int j = 0; j < record.position.pagename.length(); j++) {
            if (record.position.pagename.charAt(j) >= '0' && record.position.pagename.charAt(j) <= '9') {
                indx = j;
                break;
            }
        }
        indx = Integer.parseInt(record.position.pagename.substring(indx));
        updateAllIndexesAfterDelete(record.row, indx, BPTrees, RTrees);

    }

    public int delete(Hashtable<String, Object> row, int index, ArrayList<BPTree> BPTrees, ArrayList<RTree> RTrees) throws DBAppException {
        Object valueOfKey = row.get(key);
        int deleted = 0;
        int lo = 0;
        int hi = Rows.size() - 1;
        int mid = (hi + lo) / 2;
        while (lo <= hi) {// gowa el while bn delete
            mid = (lo + hi) / 2;
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) > 0) {
                hi = mid - 1;
            }
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) < 0) {
                lo = mid + 1;
            }
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) == 0) {
                // awel mabala2y 7aga shabahy baroo7 lawel 7aga menha w a check
                // lw ha deleteha wala la2 w ashta3'al
                int i = mid;
                for (; i >= 0; i--) {
                    if (Compare(Rows.get(i).row.get(key), valueOfKey, key, table) != 0) {
                        break;
                    }
                }
                i++;
                if (Compare(Rows.get(i).row.get(key), valueOfKey, key, table) != 0) {
                    // sometimes index is before the one I shall start from
                    i++;
                }
                int j = Math.max(i, 0);
                int size = Rows.size();
                for (; j < size; j++) {
                    if (Compare(Rows.get(i).row.get(key), valueOfKey, key, table) != 0) {
                        break;
                    }
                    Set<String> keys = row.keySet();
                    boolean identical = true;
                    for (String k : keys) {
                        String typeOfKey = getType(table, k);
                        if (typeOfKey.equals("java.awt.Polygon")) {
                            if (!Rows.get(i).row.get(k).equals(row.get(k))) {
                                identical = false;
                                i++;
                                break;
                            }
                        } else {
                            if (Compare(Rows.get(i).row.get(k), row.get(k), k, table) != 0) {
                                identical = false;
                                i++;
                                break;
                            }
                        }
                    }
                    if (identical) {
                        Ref r = new Ref(table + index, i);
                        Hashtable<String, Object> deletedrow = Rows.get(i).row;
                        deleteFromIndex(deletedrow, r, BPTrees, RTrees);
                        Rows.remove(i);
                        updateAllIndexesAfterDelete(deletedrow, index, BPTrees, RTrees);
                        deleted++;
                    }

                }
                return deleted;

            }
            //updateAllIndexesAfterDelete(row, index, BPTrees,RTrees);

        }
        return deleted;

    }

    public int linearDelete(Hashtable<String, Object> row, ArrayList<BPTree> BPTrees, int index, ArrayList<RTree> RTrees) throws DBAppException {
        int countDeleted = 0;
        Set<String> keys = row.keySet();
        int size = Rows.size() - 1;
        for (int i = size; i >= 0; i--) {
            boolean identical = true;
            for (String k : keys) {
                String typeOfKey = getType(table, k);
                if (typeOfKey.equals("java.awt.Polygon")) {
                    if (!Rows.get(i).row.get(k).equals(row.get(k))) {
                        identical = false;
                        break;
                    }
                }
                if (Compare(Rows.get(i).row.get(k), row.get(k), k, table) != 0) {
                    identical = false;
                    break;
                }
            }
            if (identical) {
                countDeleted++;
                Ref r = new Ref(table + index, i);
                Hashtable<String, Object> deletedrow = Rows.get(i).row;
                deleteFromIndex(deletedrow, r, BPTrees, RTrees);
                Rows.remove(i);
                updateAllIndexesAfterDelete(deletedrow, index, BPTrees, RTrees);

            }
        }


        return countDeleted;
    }

    public int update(Hashtable<String, Object> row, int index, ArrayList<BPTree> BPTrees, ArrayList<RTree> RTrees) throws DBAppException {
        Object valueOfKey = row.get(key);
        int lo = 0;
        int updated = 0;
        int hi = Rows.size() - 1;
        int mid = (hi + lo) / 2;
        while (lo <= hi) {// gowa el while bn update
            mid = (lo + hi) / 2;
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) > 0) {
                hi = mid - 1;
            }
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) < 0) {
                lo = mid + 1;
            }
            if (Compare(Rows.get(mid).row.get(key), valueOfKey, key, table) == 0) {
                int i = mid;
                for (; i >= 0; i--) { //ADDED AN EQUAL
                    if (Compare(Rows.get(i).row.get(key), valueOfKey, key, table) != 0) {
                        i++;
                        break;
                    }
                }
                if (Compare(Rows.get(Math.max(i, 0)).row.get(key), valueOfKey, key, table) != 0) {
                    i++;
                }
                int j = Math.max(i, 0);
                int size = Rows.size();
                for (; j < size; j++) {
                    if (Compare(Rows.get(j).row.get(key), valueOfKey, key, table) != 0) {
                        break;
                    }
                    String typeOfKey = getType(table, key);
                    if (typeOfKey.equals("java.awt.Polygon")) {
                        if (!Rows.get(j).row.get(key).equals(valueOfKey)) {
                            continue;
                        }
                    }
                    Set<String> keys = row.keySet();
                    for (String k : keys) {
                        if (k.equals(key)) continue;
                        Ref r = new Ref(table + index, j);
                        for (int m = 0; m < BPTrees.size(); m++) {
                            if (BPTrees.get(m).ColName.equals(k)) {
                                int type = getType(k);
                                if (type == 1)
                                    BPTrees.get(m).deleteSingleRef((Integer) Rows.get(j).row.get(k), r);
                                if (type == 2)
                                    BPTrees.get(m).deleteSingleRef((String) Rows.get(j).row.get(k), r);
                                if (type == 3)
                                    BPTrees.get(m).deleteSingleRef((double) Rows.get(j).row.get(k), r);
                                if (type == 4)
                                    BPTrees.get(m).deleteSingleRef((boolean) Rows.get(j).row.get(k), r);
                                if (type == 5)
                                    BPTrees.get(m).deleteSingleRef((Date) Rows.get(j).row.get(k), r);
                                //BPTrees.get(m).deleteSingleRef((Comparable)Rows.get(j).row.get(k),r );
                            }
                        }
                        for (int m = 0; m < RTrees.size(); m++) {
                            if (RTrees.get(m).ColName.equals(k)) {
                                RTrees.get(m).deleteSingleRef((DBPolygon) Rows.get(j).row.get(k), r);
                                break;
                            }
                        }
                        Rows.get(j).row.remove(k);
                        Rows.get(j).row.put(k, row.get(k));
                        r = new Ref(table + index, j);
                        for (int m = 0; m < BPTrees.size(); m++) {
                            if (BPTrees.get(m).ColName.equals(k)) {
                                int type = getType(k);
                                if (type == 1)
                                    BPTrees.get(m).insert((Integer) Rows.get(j).row.get(k), r);
                                if (type == 2)
                                    BPTrees.get(m).insert((String) Rows.get(j).row.get(k), r);
                                if (type == 3)
                                    BPTrees.get(m).insert((double) Rows.get(j).row.get(k), r);
                                if (type == 4)
                                    BPTrees.get(m).insert((boolean) Rows.get(j).row.get(k), r);
                                if (type == 5)
                                    BPTrees.get(m).insert((Date) Rows.get(j).row.get(k), r);
                                //BPTrees.get(m).insert((Comparable)Rows.get(j).row.get(k),r );
                            }
                        }
                        for (int m = 0; m < RTrees.size(); m++) {
                            if (RTrees.get(m).ColName.equals(k)) {
                                RTrees.get(m).insert((DBPolygon) Rows.get(j).row.get(k), r);
                                break;
                            }
                        }
                    }
                }
                updated++;
                break;
            }
        }
        return updated;
    }

    public void updateAt(Record record, ArrayList<BPTree> BPTrees, ArrayList<RTree> RTrees) throws DBAppException {
        Hashtable<String, Object> row = record.row;
        int j = record.position.i;
        Set<String> keys = row.keySet();
        for (String k : keys) {
            Ref r = new Ref(record.position.pagename, j);
            for (int m = 0; m < BPTrees.size(); m++) {
                if (BPTrees.get(m).ColName.equals(k)) {
                    int type = getType(k);
                    if (type == 1)
                        BPTrees.get(m).deleteSingleRef((Integer) Rows.get(j).row.get(k), r);
                    if (type == 2)
                        BPTrees.get(m).deleteSingleRef((String) Rows.get(j).row.get(k), r);
                    if (type == 3)
                        BPTrees.get(m).deleteSingleRef((double) Rows.get(j).row.get(k), r);
                    if (type == 4)
                        BPTrees.get(m).deleteSingleRef((boolean) Rows.get(j).row.get(k), r);
                    if (type == 5)
                        BPTrees.get(m).deleteSingleRef((Date) Rows.get(j).row.get(k), r);
                }
            }
            for (int m = 0; m < RTrees.size(); m++) {
                if (RTrees.get(m).ColName.equals(k)) {
                    RTrees.get(m).deleteSingleRef((DBPolygon) Rows.get(j).row.get(k), r);
                    break;
                }
            }

            Rows.get(j).row.remove(k);
            Rows.get(j).row.put(k, row.get(k));
            r = new Ref(record.position.pagename, j);
            for (int m = 0; m < BPTrees.size(); m++) {
                if (BPTrees.get(m).ColName.equals(k)) {

                    int type = getType(k);
                    if (type == 1)
                        BPTrees.get(m).insert((Integer) Rows.get(j).row.get(k), r);
                    if (type == 2)
                        BPTrees.get(m).insert((String) Rows.get(j).row.get(k), r);
                    if (type == 3)
                        BPTrees.get(m).insert((double) Rows.get(j).row.get(k), r);
                    if (type == 4)
                        BPTrees.get(m).insert((boolean) Rows.get(j).row.get(k), r);
                    if (type == 5)
                        BPTrees.get(m).insert((Date) Rows.get(j).row.get(k), r);
                }
            }
            for (int m = 0; m < RTrees.size(); m++) {
                if (RTrees.get(m).ColName.equals(k)) {
                    RTrees.get(m).insert((DBPolygon) Rows.get(j).row.get(k), r);
                    break;
                }
            }

        }
    }

    public String getType(String table, String keyCol) throws DBAppException {
        String ans = "";
        BufferedReader br;
        String s = "";
        try {
            br = new BufferedReader(new FileReader("data/metadata.csv"));
            s = br.readLine();
        } catch (FileNotFoundException e) {
            throw new DBAppException("Can not read metadata file");
        } catch (IOException IO) {
            throw new DBAppException("Failed to read metadata file");
        }
        try {
            while (br.ready()) {
                s = br.readLine();
                String[] st = s.split(", ");
                if (!st[0].equals(table) || !st[1].equals(keyCol)) continue;
                String value = st[2];
                return value;
            }
        } catch (IOException e) {
            throw new DBAppException("Failed to read metadata file");
        }
        return ans;
    }

    public int Compare(Object x, Object y, String ColumnName, String table) throws DBAppException {
        BufferedReader br;
        String s = "";
        try {
            br = new BufferedReader(new FileReader("data/metadata.csv"));
            s = br.readLine();
        } catch (FileNotFoundException e) {
            throw new DBAppException("can not read metadata file");
        } catch (IOException IO) {
            throw new DBAppException("failed to read metadata file");
        }
        try {
            while (br.ready()) {
                s = br.readLine();
                String[] st = s.split(", ");
                if (!st[0].equals(table) || !st[1].equals(ColumnName)) continue;
                String value = st[2];
                switch (value) {
                    case "java.lang.Integer":
                        return ((Integer) x).compareTo((Integer) y);
                    case "java.lang.String":
                        return (x.toString()).compareTo(y.toString());
                    case "java.lang.Double":
                        return ((Double) x).compareTo((Double) y);
                    case "java.lang.Boolean":
                        return ((Boolean) x).compareTo((Boolean) y);
                    case "java.util.Date":
                        return ((Date) x).compareTo((Date) y);
                    case "java.awt.Polygon":
//                        DBPolygon PX = new DBPolygon((Polygon) x), PY = new DBPolygon((Polygon) y);
                        return ((DBPolygon) x).compareTo((DBPolygon) y);
                }
            }
        } catch (IOException e) {
            throw new DBAppException("failed to read metadata file");
        }
        return 0;
    }

    public int getType(String ColName) throws DBAppException {
        BufferedReader br;
        String s = "";
        try {
            br = new BufferedReader(new FileReader("data/metadata.csv"));
            s = br.readLine();
        } catch (FileNotFoundException e) {
            throw new DBAppException("can not find metadata file");
        } catch (IOException IO) {
            throw new DBAppException("can not write to metadata file");
        }
        try {
            while (br.ready()) {
                s = br.readLine();
                String[] st = s.split(", ");
                if (!st[0].equals(table) || !st[1].equals(ColName)) continue;
                String value = st[2];
                switch (value) {
                    case "java.lang.Integer":
                        return 1;
                    case "java.lang.String":
                        return 2;
                    case "java.lang.Double":
                        return 3;
                    case "java.lang.Boolean":
                        return 4;
                    case "java.util.Date":
                        return 5;

                }
                break;
            }
        } catch (IOException e) {
            throw new DBAppException("can't write to metadata file");
        }
        return -1;
    }

}
