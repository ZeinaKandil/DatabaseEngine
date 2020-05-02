package Nenenenene;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;

public class Table implements Serializable {
    int pages;
    String Key;
    String Name;
    ArrayList<String> PagesNames;
    @SuppressWarnings("rawtypes")
    ArrayList<BPTree> BPTrees;
    ArrayList<RTree> RTrees;
    Properties config;
    int N;

    public void readProperty() throws DBAppException {
        config = new Properties();
        try {
            FileInputStream f = new FileInputStream("config/DBApp.properties");
            config.load(f);
        } catch (IOException e) {
            throw new DBAppException("Problem with reading the config file.");
        }

    }

    public Table(String key, String name) throws DBAppException {
        pages = 0;
        Key = key;
        Name = name;
        PagesNames = new ArrayList<>();
        BPTrees = new ArrayList<>();
        RTrees = new ArrayList<>();
        readProperty();
        String s = config.getProperty("MaximumRowsCountinPage");
        N = Integer.parseInt(s);
    }

    public Page deserialize(String name, int index) throws DBAppException {
        Page current = null;
        try {
            FileInputStream fileIn = new FileInputStream("data/" + name + index + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (Page) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e) {
            throw new DBAppException("No file with this name");
        }
        return current;
    }

    public Page deserialize(String name) throws DBAppException {
        Page current = null;
        try {
            FileInputStream fileIn = new FileInputStream("data/" + name + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (Page) in.readObject();
            in.close();
            fileIn.close();
        } catch (Exception e) {
            throw new DBAppException("No file with this name");
        }
        return current;
    }

    public void serialize(Page p, String name, int index) throws DBAppException {
        try {
            FileOutputStream fileOut = new FileOutputStream("data/" + name + index + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            throw new DBAppException("Can not serialize Page");
        }
    }

    public void serialize(Page p, String name) throws DBAppException {
        try {
            FileOutputStream fileOut = new FileOutputStream("data/" + name + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            throw new DBAppException("Can not serialize Page");
        }
    }

    public void linearDeleteFromPage(Hashtable<String, Object> HtblColNameValue) throws DBAppException {
        for (int i = 0; i < pages; i++) {
            int deleted = 1;
            while (deleted != 0) {
                if (PagesNames.size() == i) { //rokaya:I aded this line because it was giving array out of bount exception
                    break;
                }
                Page current = deserialize(Name, Integer.parseInt(PagesNames.get(i)));
                deleted = current.linearDelete(HtblColNameValue, BPTrees, Integer.parseInt(PagesNames.get(i)), RTrees);

                if (deleted != 0 && current.Rows.size() == 0) {//CONDITION FOR LAZY SHIFTING
                    serialize(current, Name, Integer.parseInt(PagesNames.get(i)));
                    shift(Integer.parseInt(PagesNames.get(i)), current.N);//SHIFT IS CALLED WITH current.N BECAUSE NOW PAGE IS EMPTY
                } else {
                    if (deleted != 0) {  //fixing excess serialization
                        serialize(current, Name, Integer.parseInt(PagesNames.get(i)));
                    }

                }

            }
        }

    }


    private static void deleteFile(String filename) {
        File f = new File(filename);
        f.delete();
    }

    public void shift(int index, int n) throws DBAppException { // it is doing the shift but it is not actually deleting from disk
        deleteFile("data/" + Name + (index) + ".class");
        PagesNames.remove(index + "");
        pages--;

    }

    public void insertIntoIndex(Hashtable<String, Object> HtblColNameValue, Ref recordrefrence) throws DBAppException {
        Set<String> keys = HtblColNameValue.keySet();
        for (String k : keys) {
            for (int i = 0; i < BPTrees.size(); i++) {
                if (BPTrees.get(i).ColName.equals(k)) {
                    int type = getType(k);
                    if (type == 1)
                        BPTrees.get(i).insert((Integer) HtblColNameValue.get(k), recordrefrence);
                    if (type == 2)
                        BPTrees.get(i).insert((String) HtblColNameValue.get(k), recordrefrence);
                    if (type == 3)
                        BPTrees.get(i).insert((double) HtblColNameValue.get(k), recordrefrence);
                    if (type == 4)
                        BPTrees.get(i).insert((boolean) HtblColNameValue.get(k), recordrefrence);
                    if (type == 5)
                        BPTrees.get(i).insert((Date) HtblColNameValue.get(k), recordrefrence);
                }
                break;
            }
        }
        for (String k : keys) {
            for (int i = 0; i < RTrees.size(); i++) {
                if (RTrees.get(i).ColName.equals(k)) {
                    RTrees.get(i).insert((DBPolygon) HtblColNameValue.get(k), recordrefrence);
                }
            }
        }


    }

    public void deleteFromIndex(Hashtable<String, Object> HtblColNameValue, Ref recordrefrence) throws DBAppException {
        Set<String> keys = HtblColNameValue.keySet();
        for (String k : keys) {
            for (int i = 0; i < BPTrees.size(); i++) {
                if (BPTrees.get(i).ColName.equals(k)) {
                    int type = getType(k);
                    if (type == 1)
                        BPTrees.get(i).deleteSingleRef((Integer) HtblColNameValue.get(k), recordrefrence);
                    if (type == 2)
                        BPTrees.get(i).deleteSingleRef((String) HtblColNameValue.get(k), recordrefrence);
                    if (type == 3)
                        BPTrees.get(i).deleteSingleRef((double) HtblColNameValue.get(k), recordrefrence);
                    if (type == 4)
                        BPTrees.get(i).deleteSingleRef((boolean) HtblColNameValue.get(k), recordrefrence);
                    if (type == 5)
                        BPTrees.get(i).deleteSingleRef((Date) HtblColNameValue.get(k), recordrefrence);

                    break;
                }
            }
        }

        for (String k : keys) {
            for (int i = 0; i < RTrees.size(); i++) {
                if (RTrees.get(i).ColName.equals(k)) {
                    RTrees.get(i).deleteSingleRef((DBPolygon) HtblColNameValue.get(k), recordrefrence);
                }
            }
        }
    }

    public void updateIndex(Hashtable<String, Object> HtblColNameValue, Ref old, Ref neww, Hashtable<String, Object> h) throws DBAppException {
        Set<String> keys = HtblColNameValue.keySet();
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
                    break;
                }
            }
        }
        for (String k : keys) {
            for (int i = 0; i < RTrees.size(); i++) {
                if (RTrees.get(i).ColName.equals(k)) {
                    RTrees.get(i).updateRef1((DBPolygon) h.get(k), old, neww);//rokaya:not shure if updateref1 or update ref
                }
            }
        }
    }

    public void insertIntoPage(Hashtable<String, Object> HtblColNameValue) throws DBAppException {
        Object valueOfKey = HtblColNameValue.get(Key);
        //CORNER CASE IF EMPTY TABLE
        if (pages == 0) {
            Page last = new Page(Key, Name, N);
            last.Rows.add(0, new Record(HtblColNameValue, new Position(0, Name + "0")));
            Ref recordrefrence = new Ref(Name + "0", 0);
            insertIntoIndex(HtblColNameValue, recordrefrence);
            pages = 1;
            PagesNames.add("0");
            serialize(last, Name, 0);
            return;
        }
        //CORNER CASES IF BIGGEST OR SMALLEST
        int mid = 0;
        Page current = deserialize(Name, Integer.parseInt(PagesNames.get(0)));
        if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) > 0) {
            int v = current.Rows.size() - 1;
            for (int i = v; i >= 0; i--) { //shifting all records after i insert in page current
                Ref old = new Ref(Name + Integer.parseInt(PagesNames.get(0)), current.Rows.get(i).position.i);
                current.Rows.get(i).position.i = (current.Rows.get(i).position.i + 1);
                Ref neww = new Ref(Name + Integer.parseInt(PagesNames.get(0)), current.Rows.get(i).position.i);
                Hashtable<String, Object> hashtable = current.Rows.get(i).row;
                updateIndex(HtblColNameValue, old, neww, hashtable);
            }

            current.Rows.add(0, new Record(HtblColNameValue, new Position(0, Name + Integer.parseInt(PagesNames.get(0)))));
            Ref recordrefrence = new Ref(Name + Integer.parseInt(PagesNames.get(0)), 0);
            insertIntoIndex(HtblColNameValue, recordrefrence);

            //SHIFTING PHASE
            if (current.Rows.size() > current.N) {//CONDITION FOR LAZY SHIFTING
                Hashtable<String, Object> h = (current.Rows.remove(current.Rows.size() - 1)).row;//remove badal add
                serialize(current, Name, Integer.parseInt(PagesNames.get(0)));//added line
                for (int i = 1; i < pages; i++) {//page.size()not-1
                    Page after = deserialize(Name, Integer.parseInt(PagesNames.get(i)));

                    int v1 = after.Rows.size() - 1;
                    for (int j = v1; j >= 0; j--) {
                        Ref old1 = new Ref(Name + Integer.parseInt(PagesNames.get(i)), after.Rows.get(j).position.i);
                        after.Rows.get(j).position.i = (after.Rows.get(j).position.i + 1);
                        Ref neww1 = new Ref(Name + Integer.parseInt(PagesNames.get(i)), after.Rows.get(j).position.i);
                        Hashtable<String, Object> hashtable = after.Rows.get(j).row;
                        updateIndex(HtblColNameValue, old1, neww1, hashtable);
                    }

                    after.Rows.add(0, new Record(h, new Position(0, Name + Integer.parseInt(PagesNames.get(i)))));
                    Ref old = new Ref(Name + (Integer.parseInt(PagesNames.get(i - 1))), current.N);
                    Ref neww = new Ref(Name + Integer.parseInt(PagesNames.get(i)), 0);
                    updateIndex(HtblColNameValue, old, neww, h);

                    if (after.Rows.size() <= after.N) {//CONDITION FOR LAZY SHIFTING
                        serialize(after, Name, Integer.parseInt(PagesNames.get(i)));
                        return;
                    }
                    h = (after.Rows.remove(after.Rows.size() - 1)).row;
                    serialize(after, Name, Integer.parseInt(PagesNames.get(i)));

                }
                current = deserialize(Name, Integer.parseInt(PagesNames.get(pages - 1)));
                if (current.Rows.size() < current.N) {//this case will never be reached
                    current.Rows.add(current.Rows.size(), new Record(h, new Position(current.Rows.size(), Name + Integer.parseInt(PagesNames.get(pages - 1)))));
                    Ref old = new Ref(Name + Integer.parseInt(PagesNames.get(pages - 2)), current.N);
                    Ref neww = new Ref(Name + Integer.parseInt(PagesNames.get(pages - 1)), 0);
                    updateIndex(HtblColNameValue, old, neww, h);

                    serialize(current, Name, Integer.parseInt(PagesNames.get(pages - 1)));
                    return;
                } else {//creating new page
                    serialize(current, Name, Integer.parseInt(PagesNames.get(pages - 1)));//fixing
                    Page last = new Page(Key, Name, N);
                    last.Rows.add(0, new Record(h, new Position(0, Name + (Integer.parseInt(PagesNames.get(pages - 1)) + 1))));
                    Ref old = new Ref(Name + Integer.parseInt(PagesNames.get(pages - 1)), current.N);
                    Ref neww = new Ref(Name + (Integer.parseInt(PagesNames.get(pages - 1)) + 1), 0);
                    updateIndex(HtblColNameValue, old, neww, h);
                    serialize(last, Name, Integer.parseInt(PagesNames.get(pages - 1)) + 1);
                    PagesNames.add(Integer.parseInt(PagesNames.get(pages - 1)) + 1 + "");
                    pages++;
                }
            } else {//no need for shifting
                serialize(current, Name, Integer.parseInt(PagesNames.get(0)));
            }

            return;
        } else {

            serialize(current, Name, Integer.parseInt(PagesNames.get(0)));  //fixing
            current = deserialize(Name, Integer.parseInt(PagesNames.get(pages - 1)));
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) < 0) {
                if (current.Rows.size() < current.N) {
                    int x = current.Rows.size();
                    current.Rows.add(x, new Record(HtblColNameValue, new Position(x, Name + Integer.parseInt(PagesNames.get(pages - 1)))));
                    Ref recordrefrence = new Ref(Name + Integer.parseInt(PagesNames.get(pages - 1)), x);
                    insertIntoIndex(HtblColNameValue, recordrefrence);
                    serialize(current, Name, Integer.parseInt(PagesNames.get(pages - 1)));
                } else {//BIGGER THAN THE BIGGEST WHICH IS IN THE LAST ROW IN THE LAST PAGE SO WE CREATE A NEW PAGE
                    serialize(current, Name, Integer.parseInt(PagesNames.get(pages - 1)));
                    Page last = new Page(Key, Name, N);
                    last.Rows.add(0, new Record(HtblColNameValue, new Position(0, Name + (Integer.parseInt(PagesNames.get(pages - 1)) + 1))));
                    Ref recordrefrence = new Ref(Name + (Integer.parseInt(PagesNames.get(pages - 1)) + 1), 0);
                    insertIntoIndex(HtblColNameValue, recordrefrence);
                    PagesNames.add("" + (Integer.parseInt(PagesNames.get(pages - 1)) + 1));
                    pages++;
                    serialize(last, Name, Integer.parseInt(PagesNames.get(pages - 1)));
                }

                return;
            }

            if (ifIndexedBP(Key) || ifIndexedR(Key)) {
                insertByIndex(HtblColNameValue);
                return;
            }
        }
        int lo = 0;
        int hi = pages - 1;
        mid = (lo + hi) / 2;
        while (lo <= hi) {
            mid = (lo + hi) / 2;
            current = deserialize(Name, Integer.parseInt(PagesNames.get(mid)));
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) < 0) {
                lo = mid + 1;
                //serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));   fixing
                continue;
            }
            if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) > 0) {
                hi = mid - 1;
            } else {
                //serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));//added line   fixing
                break;
            }
            //serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));     fixing

        }
        current = deserialize(Name, Integer.parseInt(PagesNames.get(mid)));//added line
        
        int index = current.insert(HtblColNameValue, Integer.parseInt(PagesNames.get(mid)), BPTrees, RTrees);
        //serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));


        //SHIFTING PHASE
        if (current.Rows.size() > current.N) {//CONDITION FOR LAZY SHIFTING
            Hashtable<String, Object> h = (current.Rows.remove(current.Rows.size() - 1)).row;//remove badal add
            serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));//added line
            for (int i = mid + 1; i < pages; i++) {//page.size()not-1
                Page after = deserialize(Name, Integer.parseInt(PagesNames.get(i)));

                int v1 = after.Rows.size() - 1;
                for (int j = v1; j >= 0; j--) {
                    Ref old1 = new Ref(Name + Integer.parseInt(PagesNames.get(i)), after.Rows.get(j).position.i);
                    after.Rows.get(j).position.i = (after.Rows.get(j).position.i + 1);
                    Ref neww1 = new Ref(Name + Integer.parseInt(PagesNames.get(i)), after.Rows.get(j).position.i);
                    Hashtable<String, Object> hashtable = after.Rows.get(j).row;
                    updateIndex(HtblColNameValue, old1, neww1, hashtable);
                }
                after.Rows.add(0, new Record(h, new Position(0, Name + Integer.parseInt(PagesNames.get(i)))));
                Ref old = new Ref(Name + Integer.parseInt(PagesNames.get(i - 1)), current.N);
                Ref neww = new Ref(Name + Integer.parseInt(PagesNames.get(i)), 0);
                updateIndex(HtblColNameValue, old, neww, h);

                if (after.Rows.size() <= after.N) {//CONDITION FOR LAZY SHIFTING
                    serialize(after, Name, Integer.parseInt(PagesNames.get(i)));
                    return;
                }
                h = (after.Rows.remove(after.Rows.size() - 1)).row;
                serialize(after, Name, Integer.parseInt(PagesNames.get(i)));

            }
            current = deserialize(Name, Integer.parseInt(PagesNames.get(pages - 1)));
            if (current.Rows.size() < current.N) {//not reached
                current.Rows.add(current.Rows.size(), new Record(h, new Position(current.Rows.size(), Name + Integer.parseInt(PagesNames.get(pages - 1)))));
                Ref old = new Ref(Name + Integer.parseInt(PagesNames.get(pages - 2)), current.N);
                Ref neww = new Ref(Name + Integer.parseInt(PagesNames.get(pages - 1)), 0);
                updateIndex(HtblColNameValue, old, neww, h);
                serialize(current, Name, Integer.parseInt(PagesNames.get(pages - 1)));
                return;
            } else {//creating new page
                serialize(current, Name, Integer.parseInt(PagesNames.get(pages - 1)));  //fixing
                Page last = new Page(Key, Name, N);
                last.Rows.add(0, new Record(h, new Position(0, Name + (Integer.parseInt(PagesNames.get(pages - 1)) + 1))));

                Ref old = new Ref(Name + Integer.parseInt(PagesNames.get(pages - 1)), current.N);
                Ref neww = new Ref(Name + (Integer.parseInt(PagesNames.get(pages - 1)) + 1), 0);
                updateIndex(HtblColNameValue, old, neww, h);
                serialize(last, Name, Integer.parseInt(PagesNames.get(pages - 1)) + 1);
                PagesNames.add(Integer.parseInt(PagesNames.get(pages - 1)) + 1 + "");
                pages++;
            }
        } else {
            serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));  //fixing
        }
    }

    public void insertByIndex(Hashtable<String, Object> HtblColNameValue) throws DBAppException {
        Object valueOfKey = HtblColNameValue.get(Key);
        ArrayList<Ref> r = new ArrayList<>();
        if (ifIndexedBP(Key)) {
            for (int i = 0; i < BPTrees.size(); i++) {
                if (BPTrees.get(i).ColName.equals(Key)) {
                    r = BPTrees.get(i).searchforInsert((Comparable) valueOfKey);
                    break;
                }
            }
        } else if (ifIndexedR(Key)) {
            for (int i = 0; i < RTrees.size(); i++) {
                if (RTrees.get(i).ColName.equals(Key)) {
                    r = RTrees.get(i).searchforInsert((DBPolygon) valueOfKey);
                    break;
                }
            }
        }
        Page current;
        int min = Integer.parseInt(PagesNames.get(PagesNames.size() - 1)) + 1;
        int max = -1;
        for (int i = 0; i < r.size(); i++) {
            String pageName = r.get(i).pageName;
            int num = -1;
            for (int j = 0; j < pageName.length(); j++) {
                if (pageName.charAt(j) >= '0' && pageName.charAt(j) <= '9') {
                    num = j;
                    break;
                }
            }
            num = Integer.parseInt(pageName.substring(num));
            max = Math.max(max, num);
            min = Math.min(min, num);
        }
        // if i only found keys bigger than me then insert in the first page of key bigger
        current = deserialize(Name + min);
        if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) > 0) {
            int index = current.insert(HtblColNameValue, min, BPTrees, RTrees);
            RokayaShiftingAfterInsert(min, HtblColNameValue, current);//current is serialized here
            return;

        }
        // insert in the last page having keys less than or equal me
        current = deserialize(Name + max);
        int index = current.insert(HtblColNameValue, max, BPTrees, RTrees);
        RokayaShiftingAfterInsert(max, HtblColNameValue, current);//current is serialized here
        return;


    }

    public void RokayaShiftingAfterInsert(int mid, Hashtable<String, Object> HtblColNameValue, Page current) throws DBAppException {
        if (current.Rows.size() > current.N) {//CONDITION FOR LAZY SHIFTING
            Hashtable<String, Object> h = (current.Rows.remove(current.Rows.size() - 1)).row;//remove badal add
            serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));//added line
            for (int i = mid + 1; i < pages; i++) {//page.size()not-1
                Page after = deserialize(Name, Integer.parseInt(PagesNames.get(i)));

                int v1 = after.Rows.size() - 1;
                for (int j = v1; j >= 0; j--) {
                    Ref old1 = new Ref(Name + Integer.parseInt(PagesNames.get(i)), after.Rows.get(j).position.i);
                    after.Rows.get(j).position.i = (after.Rows.get(j).position.i + 1);
                    Ref neww1 = new Ref(Name + Integer.parseInt(PagesNames.get(i)), after.Rows.get(j).position.i);
                    Hashtable<String, Object> hashtable = after.Rows.get(j).row;
                    updateIndex(HtblColNameValue, old1, neww1, hashtable);
                }
                after.Rows.add(0, new Record(h, new Position(0, Name + Integer.parseInt(PagesNames.get(i)))));
                Ref old = new Ref(Name + Integer.parseInt(PagesNames.get(i - 1)), current.N);
                Ref neww = new Ref(Name + Integer.parseInt(PagesNames.get(i)), 0);
                updateIndex(HtblColNameValue, old, neww, h);

                if (after.Rows.size() <= after.N) {//CONDITION FOR LAZY SHIFTING
                    serialize(after, Name, Integer.parseInt(PagesNames.get(i)));
                    return;
                }
                h = (after.Rows.remove(after.Rows.size() - 1)).row;
                serialize(after, Name, Integer.parseInt(PagesNames.get(i)));

            }
            current = deserialize(Name, Integer.parseInt(PagesNames.get(pages - 1)));
            if (current.Rows.size() < current.N) {//not reached
                current.Rows.add(current.Rows.size(), new Record(h, new Position(current.Rows.size(), Name + Integer.parseInt(PagesNames.get(pages - 1)))));
                Ref old = new Ref(Name + Integer.parseInt(PagesNames.get(pages - 2)), current.N);
                Ref neww = new Ref(Name + Integer.parseInt(PagesNames.get(pages - 1)), 0);
                updateIndex(HtblColNameValue, old, neww, h);
                serialize(current, Name, Integer.parseInt(PagesNames.get(pages - 1)));
                return;
            } else {//creating new page
                serialize(current, Name, Integer.parseInt(PagesNames.get(pages - 1)));
                Page last = new Page(Key, Name, N);
                last.Rows.add(0, new Record(h, new Position(0, Name + (Integer.parseInt(PagesNames.get(pages - 1)) + 1))));

                Ref old = new Ref(Name + Integer.parseInt(PagesNames.get(pages - 1)), current.N);
                Ref neww = new Ref(Name + (Integer.parseInt(PagesNames.get(pages - 1)) + 1), 0);
                updateIndex(HtblColNameValue, old, neww, h);
                serialize(last, Name, Integer.parseInt(PagesNames.get(pages - 1)) + 1);
                PagesNames.add(Integer.parseInt(PagesNames.get(pages - 1)) + 1 + "");
                pages++;
            }
        } else {
            serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));
        }
    }

    public void deleteFromPage(Hashtable<String, Object> HtblColNameValue) throws DBAppException {
        if (ifIndexedBP(Key) || ifIndexedR(Key)) {
            deleteIfExistIndex(HtblColNameValue, Key);
            return;
        }

        for (String s : HtblColNameValue.keySet()) {  //I din't see shiting there yet
            if (ifIndexedBP(s) || ifIndexedR(s)) {
                deleteIfExistIndex(HtblColNameValue, s);
                return;
            }
        }
        if (!HtblColNameValue.containsKey(Key)) {
            linearDeleteFromPage(HtblColNameValue);
            return;
        }
        Object valueOfKey = HtblColNameValue.get(Key);
        int lo = 0;
        int hi = pages - 1;
        int mid = (lo + hi) / 2;
        boolean found = false;
        while (lo <= hi && !found) {
            mid = (lo + hi) / 2;
            Page current = deserialize(Name, Integer.parseInt(PagesNames.get(mid)));
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) < 0) {
                lo = mid + 1;
                serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));
                continue;
            }
            if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) > 0) {
                hi = mid - 1;
            }
            if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) <= 0
                    && Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) >= 0) {
                found = true;
            }
            serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));
        }
        //if (!found)
        //throw new DBAppException("This record does not exist");
        int i = mid;
        for (; i >= 0; i--) {
            Page current = deserialize(Name, Integer.parseInt(PagesNames.get(i)));
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) < 0) {
                serialize(current, Name, Integer.parseInt(PagesNames.get(i)));
                //i++;
                break;
            }
            serialize(current, Name, Integer.parseInt(PagesNames.get(i)));
        }
        i++;
        int size = pages;
        for (int j = i; j < pages; j++) {
            Page current = null;
            int deleted = 1;
            boolean flag = false;
            while (deleted != 0 && j < pages) {
                current = deserialize(Name, Integer.parseInt(PagesNames.get(j)));
                flag = false;
                deleted = current.delete(HtblColNameValue, Integer.parseInt(PagesNames.get(j)), BPTrees, RTrees);
                if (deleted != 0) {
                    flag = true;
                    if (current.Rows.size() == 0) {//CONDITION FOR LAZY SHIFTING
                        serialize(current, Name, Integer.parseInt(PagesNames.get(j)));
                        shift(Integer.parseInt(PagesNames.get(j)), current.N);//SHIFT IS CALLED WITH current.N BECAUSE NOW PAGE IS EMPTY
                    } else
                        serialize(current, Name, Integer.parseInt(PagesNames.get(j)));
                }
            }
            if (!flag)
                serialize(current, Name, Integer.parseInt(PagesNames.get(j)));
        }
    }

    public void deleteIfExistIndex(Hashtable<String, Object> HtblColNameValue, String FirstInd) throws DBAppException {//we need to do .equals on polygon like the normal delete
        ArrayList<Record> result = selectFromTable(FirstInd, HtblColNameValue.get(FirstInd), "=");
        if (result.size() == 0) {
            throw new DBAppException("record not found");
        }
        ArrayList<Record> finalResult = new ArrayList<>();
        for (int i = 0; i < result.size(); i++) {
            boolean flag = true;
            for (String key : HtblColNameValue.keySet()) {
            	if (!(HtblColNameValue.get(key).equals(result.get(i).row.get(key)))) {
                    flag = false;
                    break;
                }
            }
            if (flag)
                finalResult.add(result.get(i));
        }
        final boolean[] er = {false};
        Collections.sort(finalResult, new Comparator<Record>() {
            @Override
            public int compare(Record o1, Record o2) {


                try {
                    if (Compare(o1.row.get(Key), o2.row.get(Key)) != 0)
                        return Compare(o1.row.get(Key), o2.row.get(Key));

                    int i = 0;
                    for (; i < o1.position.pagename.length(); i++) {
                        if (o1.position.pagename.charAt(i) >= '0' && o1.position.pagename.charAt(i) <= '9')
                            break;
                    }
                    int ind1 = Integer.parseInt(o1.position.pagename.substring(i));
                    int ind2 = Integer.parseInt(o2.position.pagename.substring(i));
                    if (ind1 != ind2)
                        return ind1 - ind2;
                    return o1.position.i - o2.position.i;


                } catch (DBAppException e) {

                    e.printStackTrace();
                    er[0] = true;
                    return 0;
                }

            }

        });
        if (er[0])
            throw new DBAppException("something went wrong when comparing clustering keys");
        for (int i = finalResult.size() - 1; i >= 0; i--) {
            Record now = finalResult.get(i);
            Page current = deserialize(now.position.pagename);
            int indx = 0;
            for (int j = 0; j < now.position.pagename.length(); j++) {
                if (now.position.pagename.charAt(j) >= '0' && now.position.pagename.charAt(j) <= '9') {
                    indx = j;
                    break;
                }
            }
            indx = Integer.parseInt(now.position.pagename.substring(indx));
            current.deleteAt(now, BPTrees, RTrees);
            if (current.Rows.size() == 0) {
                serialize(current, now.position.pagename);
                shift(indx, 0);
            } else
                serialize(current, now.position.pagename);
        }


    }
//    public void updateAllIndexesAfterDelete(Hashtable<String, Object> row,ArrayList<String> pages) throws DBAppException {
//       	for(int x=0;x<pages.size();x++) {
//       		Page p=deserialize(pages.get(x));
//    	for(int i=0;i<p.Rows.size();i++) {
//    		Ref old =new Ref(pages.get(x),p.Rows.get(i).position.i);
//    		p.Rows.get(i).position.i=i;
//    		Ref neww=new Ref(pages.get(x), p.Rows.get(i).position.i);
//    		Set<String> keys = row.keySet();
//    		for (String k : keys) {
//				for (int j = 0; j < BPTrees.size(); j++) {
//					if (BPTrees.get(j).ColName.equals(k)) {
//						int type = getType(k);
//						if (type == 1)
//							BPTrees.get(j).updateRef1((Integer) row.get(k),old, neww);
//						if (type == 2)
//							BPTrees.get(j).updateRef1((String) row.get(k), old,neww);
//						if (type == 3)
//							BPTrees.get(j).updateRef1((double) row.get(k), old, neww);
//						if (type == 4)
//							BPTrees.get(j).updateRef1((boolean) row.get(k),old, neww);
//						if (type == 5)
//							BPTrees.get(j).updateRef1((Date) row.get(k), old,neww);
//						break;
//					}
//				}
//				for (int j = 0; j < RTrees.size(); j++) {
//					if(RTrees.get(j).ColName.equals(k)){
//						RTrees.get(j).updateRef1((DBPolygon) row.get(k), old, neww);
//						break;
//					}
//				}
//    		}
//    	}
//    	}
//    }

    public void updateIfIndexed(Hashtable<String, Object> HtblColNameValue) throws DBAppException {
        ArrayList<Record> result = selectFromTable(Key, HtblColNameValue.get(Key), "=");
        if (result.size() == 0) {
            throw new DBAppException("record not found: " + HtblColNameValue);
        }
        for (int i = 0; i < result.size(); i++) {
            Record now = result.get(i);
            now.row = HtblColNameValue;
            Page current = deserialize(now.position.pagename);
            current.updateAt(now, BPTrees, RTrees);
            serialize(current, now.position.pagename);
        }
    }

    public void updatePage(Hashtable<String, Object> HtblColNameValue) throws DBAppException {
        if (ifIndexedBP(Key) || ifIndexedR(Key)) {//i did not handle shifting here yet because i don't understand
            updateIfIndexed(HtblColNameValue);
            return;
        }
        Object valueOfKey = HtblColNameValue.get(Key);
        int lo = 0;
        int hi = pages - 1;
        int mid = (lo + hi) / 2;
        boolean found = false;
        while (lo <= hi && !found) {
            mid = (lo + hi) / 2;
            Page current = deserialize(Name, Integer.parseInt(PagesNames.get(mid)));
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) < 0) {
                lo = mid + 1;
                serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));
                continue;
            }
            if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) > 0) {
                hi = mid - 1;
            }
            if (Compare(current.Rows.get(0).row.get(Key), valueOfKey) <= 0
                    && Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) >= 0) {
                found = true;
            }
            serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));
        }
        if (!found)
            throw new DBAppException("This record does not exist");
        int i = mid;
        for (; i >= 0; i--) {
            Page current = deserialize(Name, Integer.parseInt(PagesNames.get(i)));
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), valueOfKey) < 0) {
                serialize(current, Name, Integer.parseInt(PagesNames.get(i)));
                i++;
                break;
            }
            serialize(current, Name, Integer.parseInt(PagesNames.get(i)));
        }
        int size = pages;
        int updated = 1;
        for (int j = Math.max(i, 0); j < size && updated != 0; j++) {
            Page current = null;
            boolean flag = false;
            current = deserialize(Name, Integer.parseInt(PagesNames.get(j)));
            flag = false;
            updated = current.update(HtblColNameValue, Integer.parseInt(PagesNames.get(j)), BPTrees, RTrees);
            if (updated != 0) {
                flag = true;
                serialize(current, Name, Integer.parseInt(PagesNames.get(j)));
            }
            if (!flag)
                serialize(current, Name, Integer.parseInt(PagesNames.get(j)));
        }

    }


    public int Compare(Object x, Object y) throws DBAppException {
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
                if (!st[0].equals(Name) || !st[1].equals(Key)) continue;
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
                break;
            }
        } catch (Exception e) {
            throw new DBAppException("can't write to metadata file or datatypes are incorrect");
        }
        return 0;
    }

    public int CompareInCol(String ColName, Object x, Object y) throws DBAppException {
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
                if (!st[0].equals(Name) || !st[1].equals(ColName)) continue;
                String value = st[2];
                switch (value) {
                    case "java.lang.Integer":
                        return ((Integer) x).compareTo((Integer) y);
                    case "java.lang.String":
                        return ((String) x).compareTo((String) y);
                    case "java.lang.Double":
                        return ((Double) x).compareTo((Double) y);
                    case "java.lang.Boolean":
                        return ((Boolean) x).compareTo((Boolean) y);
                    case "java.util.Date":
                        return ((Date) x).compareTo((Date) y);
                    case "java.awt.Polygon":
                        DBPolygon PX = new DBPolygon((Polygon) x), PY = new DBPolygon((Polygon) y);
                        return (PX).compareTo(PY);
                }
                break;
            }
        } catch (Exception e) {
            throw new DBAppException("can't write to metadata file or datatypes are incorrect");
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
                if (!st[0].equals(Name) || !st[1].equals(ColName)) continue;
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
        } catch (Exception e) {
            throw new DBAppException("can't write to metadata file or type entered is iincorrecyt");
        }
        return -1;
    }


    public ArrayList<Record> selectFromTable(String ColName, Object value, String operator) throws DBAppException {
        boolean indexed = ifIndexedBP(ColName) || ifIndexedR(ColName);
        boolean key = ifClusteringKey(ColName);
        ArrayList<Record> ans = new ArrayList<>();
        if (operator.equals("!=") || !(indexed || key)) {
            for (int i = 0; i < pages; i++) {
                Page cur = deserialize(Name, Integer.parseInt(PagesNames.get(i)));
                for (int j = 0; j < cur.Rows.size(); j++) {
                    Object originalValue = cur.Rows.get(j).row.get(ColName);
                    int comparison = CompareInCol(ColName, originalValue, value);
                    switch (operator) {
                        case "=":
                            if (comparison == 0)
                                ans.add(cur.Rows.get(j));
                            break;
                        case "!=":
                            if (comparison != 0)
                                ans.add(cur.Rows.get(j));
                            break;
                        case ">":
                            if (getType(ColName) == 4)
                                throw new DBAppException("Invalid operator in a column of type boolean: " + operator);
                            if (comparison > 0)
                                ans.add(cur.Rows.get(j));
                            break;
                        case "<":
                            if (getType(ColName) == 4)
                                throw new DBAppException("Invalid operator in a column of type boolean: " + operator);
                            if (comparison < 0)
                                ans.add(cur.Rows.get(j));
                            break;
                        case ">=":
                            if (getType(ColName) == 4)
                                throw new DBAppException("Invalid operator in a column of type boolean: " + operator);
                            if (comparison >= 0)
                                ans.add(cur.Rows.get(j));
                            break;
                        case "<=":
                            if (getType(ColName) == 4)
                                throw new DBAppException("Invalid operator in a column of type boolean: " + operator);
                            if (comparison <= 0)
                                ans.add(cur.Rows.get(j));
                            break;


                    }
                }
                serialize(cur, Name, Integer.parseInt(PagesNames.get(i)));
            }
            return ans;
        }
        if (indexed) {
            switch (operator) {
                case "=":
                    ans = EqualToInIndexed(value, ColName);
                    break;
                case ">":
                    if (getType(ColName) == 4)
                        throw new DBAppException("Invalid operator in a column of type boolean: " + operator);
                    ans = BiggerThanInIndexed(value, ColName);
                    break;
                case "<":
                    if (getType(ColName) == 4)
                        throw new DBAppException("Invalid operator in a column of type boolean: " + operator);
                    ans = LessThanInIndexed(value, ColName);
                    break;
                case ">=":
                    if (getType(ColName) == 4)
                        throw new DBAppException("Invalid operator in a column of type boolean: " + operator);
                    if (ifIndexedR(ColName)) {
                        ans = LessThanOrEqual(value, ColName);
                    } else {
                        ans = CompareOR(BiggerThanInIndexed(value, ColName), EqualToInIndexed(value, ColName));
                    }
                    break;
                case "<=":
                    if (getType(ColName) == 4)
                        throw new DBAppException("Invalid operator in a column of type boolean: " + operator);
                    if (ifIndexedR(ColName)) {
                        ans = BiggerThanOrEqual(value, ColName);
                    } else {
                        ans = CompareOR(LessThanInIndexed(value, ColName), EqualToInIndexed(value, ColName));
                    }
                    break;


            }
            return ans;
        }
        switch (operator) {
            case "=":
                ans = EqualToClustering(value);
                break;
            case ">":
                if (getType(ColName) == 4)
                    throw new DBAppException("Invalid operator in a column of type boolean: " + operator);
                ans = BiggerThanClustering(value);
                break;
            case "<":
                if (getType(ColName) == 4)
                    throw new DBAppException("Invalid operator in a column of type boolean: " + operator);
                ans = LessThanClustering(value);
                break;
            case ">=":
                if (getType(ColName) == 4)
                    throw new DBAppException("Invalid operator in a column of type boolean: " + operator);
                ans = CompareOR(BiggerThanClustering(value), EqualToClustering(value));
                break;
            case "<=":
                if (getType(ColName) == 4)
                    throw new DBAppException("Invalid operator in a column of type boolean: " + operator);
                ans = CompareOR(LessThanClustering(value), EqualToClustering(value));
                break;


        }
        return ans;


    }

    public ArrayList<Record> CompareAnd(ArrayList<Record> x, ArrayList<Record> y) {
        ArrayList<Record> ans = new ArrayList<>();
        HashSet<Hashtable<String, Object>> test = new HashSet<>();
        for (int i = 0; i < x.size(); i++) {
            test.add(x.get(i).row);
        }
        for (int i = 0; i < y.size(); i++) {
            if (test.contains(y.get(i).row))
                ans.add(y.get(i));
        }
        return ans;
    }

    public ArrayList<Record> CompareOR(ArrayList<Record> x, ArrayList<Record> y) {
        ArrayList<Record> ans = new ArrayList<>();
        HashSet<Hashtable<String, Object>> test = new HashSet<>();
        for (int i = 0; i < x.size(); i++) {
            ans.add(x.get(i));
            test.add(x.get(i).row);
        }
        for (int i = 0; i < y.size(); i++) {
            if (!test.contains(y.get(i).row))
                ans.add(y.get(i));
        }

        return ans;
    }

    public ArrayList<Record> CompareXOR(ArrayList<Record> x, ArrayList<Record> y) {
        ArrayList<Record> ans = new ArrayList<>();
        HashSet<Hashtable<String, Object>> test = new HashSet<>();
        HashSet<Hashtable<String, Object>> test2 = new HashSet<>();
        for (int i = 0; i < x.size(); i++) {
            test.add(x.get(i).row);
        }
        for (int i = 0; i < y.size(); i++) {
            test2.add(y.get(i).row);
        }
        for (int i = 0; i < x.size(); i++) {
            if (!test2.contains(x.get(i).row))
                ans.add(x.get(i));
        }
        for (int i = 0; i < y.size(); i++) {
            if (!test.contains(y.get(i).row))
                ans.add(y.get(i));
        }
        return ans;
    }

    public BPTree createBTreeIndex(String strColName, String type) throws DBAppException {
        BPTree x;
        switch (type) {
            case "java.lang.Integer":
                x = new BPTree<Integer>(Name, strColName);
                break;
            case "java.lang.String":
                x = new BPTree<String>(Name, strColName);
                break;
            case "java.lang.Double":
                x = new BPTree<Double>(Name, strColName);
                break;
            case "java.lang.Boolean":
                x = new BPTree<Boolean>(Name, strColName);
                break;
            case "java.util.Date":
                x = new BPTree<Date>(Name, strColName);
                break;
            default:
                throw new DBAppException("invalid type for B+index");


        }
        for (int i = 0; i < pages; i++) {
            Page curr = deserialize(Name, Integer.parseInt(PagesNames.get(i)));
            for (int j = 0; j < curr.Rows.size(); j++) {
                Ref ref = new Ref(Name + Integer.parseInt(PagesNames.get(i)), j);
                switch (type) {
                    case "java.lang.Integer":
                        x.insert((Integer) curr.Rows.get(j).row.get(strColName), ref);
                        break;
                    case "java.lang.String":
                        x.insert((String) curr.Rows.get(j).row.get(strColName), ref);
                        break;
                    case "java.lang.Double":
                        x.insert((Double) curr.Rows.get(j).row.get(strColName), ref);
                        break;
                    case "java.lang.Boolean":
                        x.insert((Boolean) curr.Rows.get(j).row.get(strColName), ref);
                        break;
                    case "java.util.Date":
                        x.insert((Date) curr.Rows.get(j).row.get(strColName), ref);
                        break;
                    default:
                        throw new DBAppException("Invalid type for B+ index");
                }

            }
        }

        BPTrees.add(x);
        return x;


    }

    public boolean ifClusteringKey(String colName) {
        return colName.equals(Key);
    }


    public boolean ifIndexedBP(String colName) {
        for (int i = 0; i < BPTrees.size(); i++) {
            if (BPTrees.get(i).ColName.equals(colName))
                return true;
        }
        return false;
    }

    public boolean ifIndexedR(String colName) {
        for (int i = 0; i < RTrees.size(); i++) {
            if (RTrees.get(i).ColName.equals(colName))
                return true;
        }
        return false;
    }

    public ArrayList<Record> LessThanClustering(Object value) throws DBAppException {
        ArrayList<Record> ans = new ArrayList<>();
        for (int i = 0; i < pages; i++) {
            Page cur = deserialize(Name, Integer.parseInt(PagesNames.get(i)));
            for (int j = 0; j < cur.Rows.size(); j++) {
                Record r = cur.Rows.get(j);
                if (Compare(r.row.get(Key), value) >= 0) {
                    serialize(cur, Name, Integer.parseInt(PagesNames.get(i)));
                    return ans;
                }
                ans.add(r);
            }
            serialize(cur, Name, Integer.parseInt(PagesNames.get(i)));
        }
        return ans;
    }

    public ArrayList<Record> BiggerThanClustering(Object value) throws DBAppException {
        ArrayList<Record> ans = new ArrayList<>();
        for (int i = pages - 1; i >= 0; i--) {
            Page cur = deserialize(Name, Integer.parseInt(PagesNames.get(i)));
            for (int j = cur.Rows.size() - 1; j >= 0; j--) {
                Record r = cur.Rows.get(j);
                if (Compare(r.row.get(Key), value) <= 0) {
                    serialize(cur, Name, Integer.parseInt(PagesNames.get(i)));
                    return ans;
                }
                ans.add(r);
            }
            serialize(cur, Name, Integer.parseInt(PagesNames.get(i)));
        }
        return ans;
    }

    public ArrayList<Record> EqualToClustering(Object value) throws DBAppException {
        ArrayList<Record> ans = new ArrayList<>();
        int index = FirstEqualInClustering(value);
        if (index == -1)
            return ans;
        for (int i = index; i < pages; i++) {
            Page cur = deserialize(Name, Integer.parseInt(PagesNames.get(i)));
            for (int j = 0; j < cur.Rows.size(); j++) {
                Record r = cur.Rows.get(j);
                if (Compare(r.row.get(Key), value) > 0) {
                    serialize(cur, Name, Integer.parseInt(PagesNames.get(i)));
                    return ans;
                }
                ans.add(r);
            }
            serialize(cur, Name, Integer.parseInt(PagesNames.get(i)));
        }

        return ans;
    }

    public int FirstEqualInClustering(Object value) throws DBAppException {
        int lo = 0;
        int hi = pages - 1;
        int mid = (lo + hi) / 2;
        int ans = -1;
        Page current;
        while (lo <= hi) {
            mid = (lo + hi) / 2;
            current = deserialize(Name, Integer.parseInt(PagesNames.get(mid)));
            if (Compare(current.Rows.get(current.Rows.size() - 1).row.get(Key), value) < 0) {//kda we will have many pages deserialised rokaya
                lo = mid + 1;
                continue;
            }
            if (Compare(current.Rows.get(0).row.get(Key), value) > 0) {
                hi = mid - 1;
            } else {
                ans = mid;
                hi = mid - 1;
            }
            serialize(current, Name, Integer.parseInt(PagesNames.get(mid)));

        }
        return ans;
    }

    public ArrayList<Record> LessThanOrEqual(Object Value, String ColName) throws DBAppException {
        RTree rTree = null;
        for (int i = 0; i < RTrees.size(); i++) {
            if (RTrees.get(i).ColName.equals(ColName)) {
                rTree = RTrees.get(i);
                break;
            }
        }
        ArrayList<Record> ans = new ArrayList<>();
        ArrayList<Ref> ansRef = new ArrayList<>();
        ansRef = rTree.searchLessOrEqual((DBPolygon) Value);
        //ZEINA - Rn I want to load each page and get all refs in it and then move on to the next page
        //I don't wanna load the same page several times
        Page now = null;
        String curPageString = "";
        if (ansRef.size() > 0) {
            curPageString = ansRef.get(0).getPage();
            now = deserialize(curPageString);
        }
        for (int i = 0; i < ansRef.size(); i++) {
            if (!curPageString.equals(ansRef.get(i).getPage())) {
                serialize(now, curPageString);
                curPageString = ansRef.get(i).getPage();
                now = deserialize(curPageString);
            }
            ans.add(now.Rows.get(ansRef.get(i).getIndexInPage()));
        }
        serialize(now, curPageString);
        //That's it
        return ans;
    }

    public ArrayList<Record> BiggerThanOrEqual(Object Value, String ColName) throws DBAppException {
        RTree rTree = null;
        for (int i = 0; i < RTrees.size(); i++) {
            if (RTrees.get(i).ColName.equals(ColName)) {
                rTree = RTrees.get(i);
                break;
            }
        }
        ArrayList<Record> ans = new ArrayList<>();
        ArrayList<Ref> ansRef = new ArrayList<>();
        ansRef = rTree.searchBiggerOrEqual((DBPolygon) Value);
        //ZEINA - Rn I want to load each page and get all refs in it and then move on to the next page
        //I don't wanna load the same page several times
        Page now = null;
        String curPageString = "";
        if (ansRef.size() > 0) {
            curPageString = ansRef.get(0).getPage();
            now = deserialize(curPageString);
        }
        for (int i = 0; i < ansRef.size(); i++) {
            if (!curPageString.equals(ansRef.get(i).getPage())) {
                serialize(now, curPageString);
                curPageString = ansRef.get(i).getPage();
                now = deserialize(curPageString);
            }
            ans.add(now.Rows.get(ansRef.get(i).getIndexInPage()));
        }
        serialize(now, curPageString);
        //That's it
        return ans;
    }

    public ArrayList<Record> LessThanInIndexed(Object Value, String ColName) throws DBAppException {
        ArrayList<Record> ans = new ArrayList<>();
        BPTree bpTree = null;
        for (int i = 0; i < BPTrees.size(); i++) {
            if (BPTrees.get(i).ColName.equals(ColName)) {
                bpTree = BPTrees.get(i);
                break;
            }
        }
        RTree rTree = null;
        for (int i = 0; i < RTrees.size(); i++) {
            if (RTrees.get(i).ColName.equals(ColName)) {
                rTree = RTrees.get(i);
                break;
            }
        }
        ArrayList<Ref> ansRef = new ArrayList<>();
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
                if (!st[0].equals(Name) || !st[1].equals(ColName)) continue;
                String value = st[2];
                switch (value) {
                    case "java.lang.Integer":
                        ansRef = bpTree.searchMin((Integer) Value);
                        break;
                    case "java.lang.String":
                        ansRef = bpTree.searchMin((String) Value);
                        break;
                    case "java.lang.Double":
                        ansRef = bpTree.searchMin((Double) Value);
                        break;
                    case "java.lang.Boolean":
                        ansRef = bpTree.searchMin((Boolean) Value);
                        break;
                    case "java.util.Date":
                        ansRef = bpTree.searchMin((Date) Value);
                        break;
                    case "java.awt.Polygon":
                        ansRef = rTree.searchMin((DBPolygon) Value);
                        break;

                }
                break;
            }
        } catch (IOException e) {
            throw new DBAppException("can't write to metadata file");
        }
//        for (int i = 0; i < ansRef.size(); i++) {
//            Page now = deserialize(ansRef.get(i).getPage());
//            ans.add(now.Rows.get(ansRef.get(i).getIndexInPage()));
//            serialize(now,ansRef.get(i).getPage());
//        }
        //ZEINA - Rn I want to load each page and get all refs in it and then move on to the next page
        //I don't wanna load the same page several times
        Page now = null;
        String curPageString = "";
        if (ansRef.size() > 0) {
            curPageString = ansRef.get(0).getPage();
            now = deserialize(curPageString);
        }
        for (int i = 0; i < ansRef.size(); i++) {
            if (!curPageString.equals(ansRef.get(i).getPage())) {
                serialize(now, curPageString);
                curPageString = ansRef.get(i).getPage();
                now = deserialize(curPageString);
            }
            ans.add(now.Rows.get(ansRef.get(i).getIndexInPage()));
        }
        serialize(now, curPageString);
        //That's it
        return ans;
    }

    public ArrayList<Record> BiggerThanInIndexed(Object Value, String ColName) throws DBAppException {
        ArrayList<Record> ans = new ArrayList<>();
        BPTree bpTree = null;
        for (int i = 0; i < BPTrees.size(); i++) {
            if (BPTrees.get(i).ColName.equals(ColName)) {
                bpTree = BPTrees.get(i);
                break;
            }
        }
        RTree rTree = null;
        for (int i = 0; i < RTrees.size(); i++) {
            if (RTrees.get(i).ColName.equals(ColName)) {
                rTree = RTrees.get(i);
                break;
            }
        }
        ArrayList<Ref> ansRef = new ArrayList<>();
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
                if (!st[0].equals(Name) || !st[1].equals(ColName)) continue;
                String value = st[2];
                switch (value) {
                    case "java.lang.Integer":
                        ansRef = bpTree.searchMax((Integer) Value);
                        break;
                    case "java.lang.String":
                        ansRef = bpTree.searchMax((String) Value);
                        break;
                    case "java.lang.Double":
                        ansRef = bpTree.searchMax((Double) Value);
                        break;
                    case "java.lang.Boolean":
                        ansRef = bpTree.searchMax((Boolean) Value);
                        break;
                    case "java.util.Date":
                        ansRef = bpTree.searchMax((Date) Value);
                        break;
                    case "java.awt.Polygon":
                        ansRef = rTree.searchMax((DBPolygon) Value);
                        break;
                }
                break;
            }
        } catch (IOException e) {
            throw new DBAppException("can't write to metadata file");
        }
//        for (int i = 0; i < ansRef.size(); i++) {
//            Page now = deserialize(ansRef.get(i).getPage());
//            ans.add(now.Rows.get(ansRef.get(i).getIndexInPage()));
//            serialize(now,ansRef.get(i).getPage());
//
//        }
        //ZEINA - Rn I want to load each page and get all refs in it and then move on to the next page
        //I don't wanna load the same page several times
        Page now = null;
        String curPageString = "";
        if (ansRef.size() > 0) {
            curPageString = ansRef.get(0).getPage();
            now = deserialize(curPageString);
        }
        for (int i = 0; i < ansRef.size(); i++) {
            if (!curPageString.equals(ansRef.get(i).getPage())) {
                serialize(now, curPageString);
                curPageString = ansRef.get(i).getPage();
                now = deserialize(curPageString);
            }
            ans.add(now.Rows.get(ansRef.get(i).getIndexInPage()));
        }
        serialize(now, curPageString);
        //That's it
        return ans;
    }

    public ArrayList<Record> EqualToInIndexed(Object Value, String ColName) throws DBAppException {

        ArrayList<Record> ans = new ArrayList<>();
        BPTree bpTree = null;
        for (int i = 0; i < BPTrees.size(); i++) {
            if (BPTrees.get(i).ColName.equals(ColName)) {
                bpTree = BPTrees.get(i);
                break;
            }
        }
        RTree rTree = null;
        for (int i = 0; i < RTrees.size(); i++) {
            if (RTrees.get(i).ColName.equals(ColName)) {
                rTree = RTrees.get(i);
                break;
            }
        }
        ArrayList<Ref> ansRef = new ArrayList<>();
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
                if (!st[0].equals(Name) || !st[1].equals(ColName)) continue;
                String value = st[2];
                switch (value) {
                    case "java.lang.Integer":
                        ansRef = bpTree.search((Integer) Value);
                        break;
                    case "java.lang.String":
                        ansRef = bpTree.search((String) Value);
                        break;
                    case "java.lang.Double":
                        ansRef = bpTree.search((Double) Value);
                        break;
                    case "java.lang.Boolean":
                        ansRef = bpTree.search((Boolean) Value);
                        break;
                    case "java.util.Date":
                        ansRef = bpTree.search((Date) Value);
                        break;
                    case "java.awt.Polygon":
                        ansRef = rTree.search((DBPolygon) Value);
                        break;
                }
                break;
            }
        } catch (IOException e) {
            throw new DBAppException("can't write to metadata file");
        }
        //ZEINA - Rn I want to load each page and get all refs in it and then move on to the next page
        //I don't wanna load the same page several times
        Page now = null;
        String curPageString = "";
        if (ansRef.size() > 0) {
            curPageString = ansRef.get(0).getPage();
            now = deserialize(curPageString);
        }
        for (int i = 0; i < ansRef.size(); i++) {
            if (!curPageString.equals(ansRef.get(i).getPage())) {
                serialize(now, curPageString);
                curPageString = ansRef.get(i).getPage();
                now = deserialize(curPageString);
            }
            ans.add(now.Rows.get(ansRef.get(i).getIndexInPage()));
        }
        if (!curPageString.equals("")) {
            serialize(now, curPageString);
        }
        //That's it
        return ans;
    }

    public RTree createRTreeIndex(String strColName) throws DBAppException {
        RTree x = new RTree(Name, strColName);
        for (int i = 0; i < pages; i++) {
            Page curr = deserialize(Name, Integer.parseInt(PagesNames.get(i)));
            for (int j = 0; j < curr.Rows.size(); j++) {
                Ref ref = new Ref(Name + Integer.parseInt(PagesNames.get(i)), j);
                x.insert((DBPolygon) curr.Rows.get(j).row.get(strColName), ref);
            }
        }

        RTrees.add(x);
        return x;
    }


}
