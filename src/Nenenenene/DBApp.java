package Nenenenene;

import java.awt.Polygon;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class DBApp {
    static Vector<Table> tables;
    static PrintWriter out;

    static {
        try {
            out = new PrintWriter(new FileOutputStream("data/metadata.csv", true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }


    public void printBPTree(int table, int btree) throws DBAppException {
        System.out.println(tables.get(table).BPTrees.get(btree).toString1());
        serializeTable();
    }

    public void printRTree(int table, int rtree) throws DBAppException {
        System.out.println(tables.get(table).RTrees.get(rtree).toString1());
        serializeTable();
    }

    public void printAllBPRef(int table, int btree) throws DBAppException {
        System.out.println(tables.get(table).BPTrees.get(btree).printAllRef());
        serializeTable();
    }

    public void printAllRRef(int table, int rtree) throws DBAppException {
        System.out.println(tables.get(table).RTrees.get(rtree).printAllRef());
    }

    public void printAllKeys(int table, int btree) throws DBAppException {
        System.out.println(tables.get(table).BPTrees.get(btree).printAllkeys());
        serializeTable();
    }

    public void printNode(int index) throws DBAppException {
        BPTreeNode x = tables.get(0).BPTrees.get(0).deserializeNode("data/" + "Humans_ID" + "NODE" + index + ".class");
        System.out.println(x);

    }


    public void init() throws DBAppException {
        // this does whatever initialization you would like
        // or leave it empty if there is no code you want to
        // execute at application startup
        try {
            FileInputStream fileIn = new FileInputStream("data/Tables.class");
            tables = deserializeTable();
            updateTrees();
        } catch (FileNotFoundException e) {
            tables = new Vector<>();
            serializeTable();
            out.println("Table Name, Column Name, Column Type, Key, Indexed");
            out.flush();
        }


    }

    public void updateTrees() throws DBAppException {
        for (int i = 0; i < tables.size(); i++) {
            Table temp = tables.get(i);
            for (int j = 0; j < temp.BPTrees.size(); j++) {
                BPTree tempB = temp.BPTrees.get(j);
                String sFileName = tempB.root.getFilePath();
                int totalLength = sFileName.length() - 6; // Removing".class"
                int TreeNameSize = tempB.getTableName().length() + 9; //"data/ + NODE"
                int index = Integer.parseInt(sFileName.substring(TreeNameSize, totalLength));
                while (true) {
                    File f = new File("data/" + tempB.getTableName() + "NODE" + ++index + ".class");
                    if (!f.exists()) {
                        tempB.root.setNextIdx(index);
                        break;
                    }
                }
            }
            for (int j = 0; j < temp.RTrees.size(); j++) {
                RTree tempR = temp.RTrees.get(j);
                String sFileName = tempR.root.getFilePath();
                int totalLength = sFileName.length() - 6; // Removing".class"
                int TreeNameSize = tempR.getTableName().length() + 11 + 5; //"data/ + RTree_Node_"
                int index = Integer.parseInt(sFileName.substring(TreeNameSize, totalLength));
                while (true) {
                    File f = new File("data/" + tempR.getTableName() + "RTree_Node_" + ++index + ".class");
                    if (!f.exists()) {
                        tempR.root.setNextIdx(index);
                        break;
                    }
                }
            }
        }
        serializeTable();
    }

    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException {
        htblColNameType.put("TouchDate", "java.util.Date");
        for (int i = 0; i < tables.size(); i++)
            if (tables.get(i).Name.equals(strTableName)) {
                serializeTable();
                throw new DBAppException("There is a table in the database with that name. PLease choose another name for your table.");
            }
        checkTypesForCreate(htblColNameType);
        Table t = new Table(strClusteringKeyColumn, strTableName);
        tables.add(t);
        //MetaData
        Set<String> keys = htblColNameType.keySet();
        for (String k : keys) {
            out.println(strTableName + ", " + k + ", " + htblColNameType.get(k) + ", " + k.equals(strClusteringKeyColumn) + ", " + false);
        }
        serializeTable();
        out.flush();

    }

    public static final long MILLISECONDS_IN_SECOND = (long) (1000);
    public static final long MILLISECONDS_IN_MINUTE = (MILLISECONDS_IN_SECOND * 60);
    public static final long MILLISECONDS_IN_HOUR = (MILLISECONDS_IN_MINUTE * 60);
    public static final long MILLISECONDS_IN_DAY = (MILLISECONDS_IN_HOUR * 24);

    public synchronized static Date roundBy(Date date, long UNIT) {
        long time = date.getTime();
        long timeOutOfUnit = time % UNIT;
        time -= timeOutOfUnit;
        if (timeOutOfUnit >= (UNIT / 2)) {
            time += UNIT;
        }
        return new Date(time);
    }

    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        boolean tableFound = false;
        for (int i = 0; i < tables.size() && !tableFound; i++) {
            if (tables.get(i).Name.equals(strTableName)) {
                tableFound = true;
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
                Date date = new Date(System.currentTimeMillis());
                Date nearestMinute = roundBy(date, MILLISECONDS_IN_SECOND);
                htblColNameValue.put("TouchDate", nearestMinute);
                try {
                    checkForInserts(htblColNameValue, strTableName);
                } catch (IOException e) {
                    serializeTable();
                    throw new DBAppException();
                }
                try {
                    tables.get(i).insertIntoPage(htblColNameValue);
                } catch (DBAppException e) {
                    serializeTable();
                    throw e;
                }
            }
        }
        serializeTable();
        if (!tableFound) {
            throw new DBAppException("No table with that name in the database");
        }
    }

    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        boolean tableFound = false;
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).Name.equals(strTableName)) {
                tableFound = true;
                try {
                    checkForDeletes(htblColNameValue, strTableName);
                } catch (IOException e) {
                    serializeTable();
                    throw new DBAppException();
                }
                try {
                    tables.get(i).deleteFromPage(htblColNameValue);
                } catch (DBAppException d) {
                    serializeTable();
                    throw d;
                }
            }
        }
        serializeTable();
        if (!tableFound) {
            serializeTable();
            throw new DBAppException("No table with that name in the database");
        }
    }

    public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        boolean tableFound = false;
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).Name.equals(strTableName)) {
                tableFound = true;
                try {
                    checkForUpdates(htblColNameValue, strTableName);
                } catch (IOException e) {
                    serializeTable();
                    throw new DBAppException();
                }
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
                Date date = new Date(System.currentTimeMillis());
                Date nearestMinute = roundBy(date, MILLISECONDS_IN_SECOND);
                htblColNameValue.put("TouchDate", nearestMinute);
                parsingKey(strTableName, strKey, htblColNameValue);
                try {
                    tables.get(i).updatePage(htblColNameValue);
                } catch (DBAppException d) {
                    serializeTable();
                    throw d;
                }

            }
        }
        serializeTable();
        if (!tableFound) {
            throw new DBAppException("No table with that name in the database");
        }
    }

    public void checkForUpdates(Hashtable<String, Object> htblColNameValue, String name) throws DBAppException, IOException {
        BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
        String s = br.readLine();
        int countKeys = 0;
        while (br.ready()) {
            s = br.readLine();
            String[] st = s.split(", ");
            if (!st[0].equals(name)) continue;
            String value = st[2];
            String k = st[1];
            boolean isKey = Boolean.parseBoolean(st[3]);
            //ZEINA CHANGED IT SO THAT THE INPUT HASHTABLE DOESN'T HAVE TO CONTAIN VALUES FOR ALL OF THE COLUMNS
            if (htblColNameValue.containsKey(k)) {
            	countKeys++;
            	if(isKey)
            		throw new DBAppException("The input Hashtable in update cannot contain the key. The key should be entered as a string");
                switch (value) {
                    case "java.lang.Integer":
                        if (!(htblColNameValue.get(k) instanceof Integer)) {
                            throw new DBAppException("This value should be an int");
                        }
                        break;
                    case "java.lang.String":
                        if (!(htblColNameValue.get(k) instanceof String)) {
                            throw new DBAppException("This value should be string");
                        }
                        break;
                    case "java.lang.Double":
                        if (!(htblColNameValue.get(k) instanceof Double)) {
                            throw new DBAppException("This value should be double");
                        }
                        break;
                    case "java.lang.Boolean":
                        if (!(htblColNameValue.get(k) instanceof Boolean)) {
                            throw new DBAppException("This value should be boolean");
                        }
                        break;
                    case "java.awt.Polygon":
                        if (!(htblColNameValue.get(k) instanceof Polygon)) {
                            throw new DBAppException("This value should be polygon");
                        }
                        else{
                        	DBPolygon db = new DBPolygon( (Polygon) htblColNameValue.get(k));
                        	htblColNameValue.remove(k);
                        	htblColNameValue.put(k, db);
                        }
                        break;
                    case "java.util.Date":
                        if (!(htblColNameValue.get(k) instanceof Date)) {
                            throw new DBAppException("This value should be date");
                        }else{
                        	Date nearestMinute = roundBy((Date)htblColNameValue.get(k), MILLISECONDS_IN_SECOND);
                            htblColNameValue.replace(k, nearestMinute);
                        }
                        break;
                    default:
                        break;
                }
            }
            if(countKeys< htblColNameValue.size())
            	throw new DBAppException("The hashTable entered has columns that do not belong to " + name);
        }
    }
    
    public void checkForDeletes(Hashtable<String, Object> htblColNameValue, String name) throws DBAppException, IOException {
        BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
        String s = br.readLine();
        int countKeys = 0;
        while (br.ready()) {
            s = br.readLine();
            String[] st = s.split(", ");
            if (!st[0].equals(name)) continue;
            String value = st[2];
            String k = st[1];
            //ZEINA CHANGED IT SO THAT THE INPUT HASHTABLE DOESN'T HAVE TO CONTAIN VALUES FOR ALL OF THE COLUMNS
            if (htblColNameValue.containsKey(k)) {
            	countKeys++;
                switch (value) {
                    case "java.lang.Integer":
                        if (!(htblColNameValue.get(k) instanceof Integer)) {
                            throw new DBAppException("This value should be an int");
                        }
                        break;
                    case "java.lang.String":
                        if (!(htblColNameValue.get(k) instanceof String)) {
                            throw new DBAppException("This value should be string");
                        }
                        break;
                    case "java.lang.Double":
                        if (!(htblColNameValue.get(k) instanceof Double)) {
                            throw new DBAppException("This value should be double");
                        }
                        break;
                    case "java.lang.Boolean":
                        if (!(htblColNameValue.get(k) instanceof Boolean)) {
                            throw new DBAppException("This value should be boolean");
                        }
                        break;
                    case "java.awt.Polygon":
                        if (!(htblColNameValue.get(k) instanceof Polygon)) {
                            throw new DBAppException("This value should be polygon");
                        }
                        else{
                        	DBPolygon db = new DBPolygon( (Polygon) htblColNameValue.get(k));
                        	htblColNameValue.remove(k);
                        	htblColNameValue.put(k, db);
                        }
                        break;
                    case "java.util.Date":
                        if (!(htblColNameValue.get(k) instanceof Date)) {
                            throw new DBAppException("This value should be date");
                        }else{
                        	Date nearestMinute = roundBy((Date)htblColNameValue.get(k), MILLISECONDS_IN_SECOND);
                            htblColNameValue.replace(k, nearestMinute);
                        }
                        break;
                    default:
                        break;
                }
            }
            if(countKeys< htblColNameValue.size())
            	throw new DBAppException("The hashTable entered has columns that do not belong to " + name);
        }
    }

    public void checkForInserts(Hashtable<String, Object> htblColNameValue, String name) throws DBAppException, IOException {
        //The TA SAID THAT FOR INSERTIONS WE NEED TO CHECK THAT ALL COLUMNS ARE INCLUDED
        BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
        String s = br.readLine();
        int countKeys = 0;
        while (br.ready()) {
            s = br.readLine();
            String[] st = s.split(", ");
            if (!st[0].equals(name)) continue;
            String value = st[2];
            String k = st[1];
            if (!htblColNameValue.containsKey(k)) {
                throw new DBAppException("Hashtable is missing columns");
            } else {
            	countKeys++;
                switch (value) {
                    case "java.lang.Integer":
                        if (!(htblColNameValue.get(k) instanceof Integer)) {
                            throw new DBAppException("This value should be an Integer");
                        }
                        break;
                    case "java.lang.String":
                        if (!(htblColNameValue.get(k) instanceof String)) {
                            throw new DBAppException("This value should be string");
                        }
                        break;
                    case "java.lang.Double":
                        if (!(htblColNameValue.get(k) instanceof Double)) {
                            throw new DBAppException("This value should be Double");
                        }
                        break;
                    case "java.lang.Boolean":
                        if (!(htblColNameValue.get(k) instanceof Boolean)) {
                            throw new DBAppException("This value should be Boolean");
                        }
                        break;
                    case "java.awt.Polygon":
                        if (!(htblColNameValue.get(k) instanceof Polygon)) {
                            throw new DBAppException("This value should be a Polygon");
                        }
                        else{
                        	DBPolygon db = new DBPolygon((Polygon) htblColNameValue.get(k));
                        	htblColNameValue.replace(k, db);
                        }
                        break;
                    case "java.util.Date":
                        if (!(htblColNameValue.get(k) instanceof Date)) {
                            throw new DBAppException("This value should be Date");
                        }else{
                        	Date nearestMinute = roundBy((Date)htblColNameValue.get(k), MILLISECONDS_IN_SECOND);
                            htblColNameValue.replace(k, nearestMinute);
                        }
                        break;

                    default:
                        break;
                }
            }
            if(countKeys< htblColNameValue.size())
            	throw new DBAppException("The hashTable entered has columns that do not belong to " + name);
        }
    }

    public void checkTypesForCreate(Hashtable<String, String> h) throws DBAppException {
        Set<String> keys = h.keySet();
        for (String k : keys) {
            switch (h.get(k)) {
                case "java.lang.Integer":
                    break;
                case "java.lang.String":
                    break;
                case "java.lang.Double":
                    break;
                case "java.lang.Boolean":
                    break;
                case "java.awt.Polygon":
                    break;
                case "java.util.Date":
                    break;
                default:
                    throw new DBAppException("You entered an invalid data type during table creation: " + h.get(k));
            }
        }
    }

    public void parsingKey(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        BufferedReader br;
        String s = "";
        try {
            BufferedReader bufferedReader = br = new BufferedReader(new FileReader("data/metadata.csv"));
            s = br.readLine();
        } catch (FileNotFoundException e) {
            throw new DBAppException("can't find meta data file");
        } catch (IOException IO) {
            throw new DBAppException("can't find metadata file");
        }
        try {
            while (br.ready()) {
                s = br.readLine();
                String[] st = s.split(", ");
                if (!st[0].equals(strTableName) || !st[3].equals("true")) continue;
                String value = st[2];

                switch (value) {
                    case "java.lang.Integer":
                        Integer x = Integer.parseInt(strKey);
                        htblColNameValue.put(st[1], x);
                        break;
                    case "java.lang.String":
                        htblColNameValue.put(st[1], strKey);
                        break;
                    case "java.lang.Double":
                        htblColNameValue.put(st[1], Double.parseDouble(strKey));
                        break;
                    case "java.lang.Boolean":
                        htblColNameValue.put(st[1], Boolean.parseBoolean(strKey));
                        break;
                    case "java.util.Date":
                        StringTokenizer stoken = new StringTokenizer(strKey, "-");
                        int year = Integer.parseInt(stoken.nextToken());
                        int month = Integer.parseInt(stoken.nextToken());
                        int days = Integer.parseInt(stoken.nextToken());
                        Calendar c = new GregorianCalendar(year, month, days);
                        Date date1 = c.getTime();
                        Date nearestMinute = roundBy(date1, MILLISECONDS_IN_SECOND);
                        htblColNameValue.put(st[1], nearestMinute);
                        break;
                    case "java.awt.Polygon":
                        StringBuilder sb = new StringBuilder();
                        int[] x1 = new int[4], y = new int[4];
                        boolean xTurn = true;
                        int counter = 0;
                        for (int i = 0; i < strKey.length(); i++) {
                            if (strKey.charAt(i) >= '0' && strKey.charAt(i) <= '9') {
                                sb.append(strKey.charAt(i));
                            } else {
                                if (sb.length() != 0) {
                                    if (xTurn) {
                                        x1[counter] = Integer.parseInt(sb.toString());
                                        xTurn = false;
                                        sb = new StringBuilder();
                                    } else {
                                        y[counter] = Integer.parseInt(sb.toString());
                                        xTurn = true;
                                        counter++;
                                        sb = new StringBuilder();
                                    }
                                }
                            }
                        }
                        Polygon pol = new Polygon(x1, y, counter);
                        DBPolygon PX = new DBPolygon(pol);
                        htblColNameValue.put(st[1], PX);
                        break;
                    default:
                        throw new DBAppException("Never happens");
                }
            }
        } catch (IOException e) {
            throw new DBAppException("cant find metadata file");
        }
    }

    //JUST T0 PRINT BEGINS
    public void printPage(String TableName, int pageNumber) throws DBAppException {
        Page p = deserialize(TableName, pageNumber);
        for (int i = 0; i < p.Rows.size(); i++) {
            System.out.println(p.Rows.get(i).row.toString() + "    page name: " + p.Rows.get(i).position.pagename + "  at index: " + p.Rows.get(i).position.i);
        }

        serialize(p, TableName, pageNumber);
    }

    public Page deserialize(String name, int index) throws DBAppException {
        Page current = null;
        try {
            FileInputStream fileIn = new FileInputStream("data/" + name + index + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = (Page) in.readObject();
            in.close();
            fileIn.close();
            return current;
        } catch (Exception i) {
            throw new DBAppException("can't find page data/" + name + index + ".class");
        }
    }

    public void serialize(Page p, String name, int index) throws DBAppException {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream("data/" + name + index + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            throw new DBAppException("can't serialize page");
        }
    }

    //JUST TO PRINT ENDS
    public Vector<Table> deserializeTable() throws DBAppException {
        Vector<Table> current = null;
        try {
            FileInputStream fileIn = new FileInputStream("data/Tables.class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            current = ((Vector<Table>) in.readObject());
            in.close();
            fileIn.close();
            return current;
        } catch (Exception i) {
            throw new DBAppException("can't find page");
        }
    }

    public void serializeTable() throws DBAppException {
        try {
            FileOutputStream fileOut = new FileOutputStream("data/Tables.class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(tables);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            throw new DBAppException("Can't serialize object.");
        }
    }

    public Table TableExists(String tableName) throws DBAppException {
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).Name.equals(tableName)) {
                return tables.get(i);
            }

        }
        return null;
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        ArrayList<Record> intermediateResult;
        Table queried = TableExists(arrSQLTerms[0].strTableName);
        if (queried == null) {
            serializeTable();
            throw new DBAppException("no such table to select from");
        }
        intermediateResult = queried.selectFromTable(arrSQLTerms[0].strColumnName, arrSQLTerms[0].objValue, arrSQLTerms[0].strOperator);
        for (int i = 1; i < arrSQLTerms.length; i++) {
            queried = TableExists(arrSQLTerms[i].strTableName);
            ArrayList<Record> currentResult;
            if (strarrOperators[i - 1].equals("AND")) {
                intermediateResult = selectAfterAnd(queried, intermediateResult, arrSQLTerms[i].strColumnName, arrSQLTerms[i].objValue, arrSQLTerms[i].strOperator);
            } else {
                currentResult = queried.selectFromTable(arrSQLTerms[i].strColumnName, arrSQLTerms[i].objValue, arrSQLTerms[i].strOperator);
                if (strarrOperators[i - 1].equals("OR"))
                    intermediateResult = queried.CompareOR(intermediateResult, currentResult);
                else intermediateResult = queried.CompareXOR(intermediateResult, currentResult);

            }
        }
        ArrayList<Hashtable<String, Object>> intermediateResult2 = new ArrayList<>();
        for (int i = 0; i < intermediateResult.size(); i++) {
            intermediateResult2.add(intermediateResult.get(i).row);
        }
        serializeTable();
        Table finalQueried = queried;
        Collections.sort(intermediateResult2, new Comparator<Hashtable<String, Object>>() {
            @Override
            public int compare(Hashtable<String, Object> o1, Hashtable<String, Object> o2) {
                try {
                    return finalQueried.Compare(o1.get(finalQueried.Key), o2.get(finalQueried.Key));
                } catch (DBAppException e) {
                    e.printStackTrace();
                }
                return -1;
            }
        });
        return intermediateResult2.iterator();


    }

    public ArrayList<Record> selectAfterAnd(Table t, ArrayList<Record> intermediate, String ColName, Object value, String op) throws DBAppException {
        ArrayList<Record> ans = new ArrayList<>();
        for (int j = 0; j < intermediate.size(); j++) {
            Object originalValue = intermediate.get(j).row.get(ColName);
            int comparison = t.CompareInCol(ColName, originalValue, value);
            switch (op) {
                case "=":
                    if (comparison == 0)
                        ans.add(intermediate.get(j));
                    break;
                case "!=":
                    if (comparison != 0)
                        ans.add(intermediate.get(j));
                    break;
                case ">":
                    if (comparison > 0)
                        ans.add(intermediate.get(j));
                    break;
                case "<":
                    if (comparison < 0)
                        ans.add(intermediate.get(j));
                    break;
                case ">=":
                    if (comparison >= 0)
                        ans.add(intermediate.get(j));
                    break;
                case "<=":
                    if (comparison <= 0)
                        ans.add(intermediate.get(j));
                    break;


            }
        }
        return ans;
    }

    public void createBTreeIndex(String strTableName, String strColName) throws DBAppException {
        boolean found = false;
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).Name.equals(strTableName)) {
                found = true;
                String type = "";
                try {
                    BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
                    StringBuilder sb = new StringBuilder();
                    String s = br.readLine();
                    sb.append(s + '\n');
                    while (br.ready()) {
                        s = br.readLine();
                        String[] st = s.split(", ");
                        if (!st[0].equals(strTableName) || !st[1].equals(strColName)) {
                            sb.append(s + '\n');
                            continue;
                        }
                        if (st[2].equals("java.awt.Polygon")) {
                            throw new DBAppException("Cannot create B+ Tree Index on type Polygon");
                        }
                        if (st[4].equals("true")) {
                            throw new DBAppException("Column is already indexed");
                        }
                        type = st[2];
                        st[4] = "true";
                        sb.append(st[0] + ", " + st[1] + ", " + st[2] + ", " + st[3] + ", " + st[4] + '\n');
                        while (br.ready()) {
                            s = br.readLine();
                            sb.append(s + "\n");
                        }
                        break;
                    }
                    PrintWriter out2 = new PrintWriter(new FileOutputStream("data/metadata.csv", false));
                    out2.print(sb.toString());
                    out2.close();

                } catch (DBAppException D) {
                    throw D;
                } catch (Exception e) {
                    throw new DBAppException("Cannot read from metadata");
                }

                if (type.equals("")) {
                    throw new DBAppException("No such column");
                }
                Table t = tables.get(i);
                t.createBTreeIndex(strColName, type);

                serializeTable();
                break;
            }
        }
        if (!found) {
            throw new DBAppException("No such table");
        }
    }


    public void createRTreeIndex(String strTableName, String strColName) throws DBAppException {
        boolean found = false;
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).Name.equals(strTableName)) {
                found = true;
                String type = "";
                try {
                    BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));
                    StringBuilder sb = new StringBuilder();
                    String s = br.readLine();
                    sb.append(s + '\n');
                    while (br.ready()) {
                        s = br.readLine();
                        String[] st = s.split(", ");
                        if (!st[0].equals(strTableName) || !st[1].equals(strColName)) {
                            sb.append(s + '\n');
                            continue;
                        }
                        if (!st[2].equals("java.awt.Polygon")) {
                            throw new DBAppException("Can't have an RTree on a type " + st[3] + " because it isn't polygon");
                        }
                        if (st[4].equals("true")) {
                            throw new DBAppException("Column is already indexed");
                        }
                        type = st[2];
                        st[4] = "true";
                        sb.append(st[0] + ", " + st[1] + ", " + st[2] + ", " + st[3] + ", " + st[4] + '\n');
                        while (br.ready()) {
                            s = br.readLine();
                            sb.append(s + "\n");
                        }
                        break;
                    }
                    PrintWriter out2 = new PrintWriter(new FileOutputStream("data/metadata.csv", false));
                    out2.print(sb.toString());
                    out2.close();

                } catch (DBAppException D) {
                    throw D;
                } catch (Exception e) {
                    throw new DBAppException("Cannot read from metadata");
                }

                if (type.equals("")) {
                    throw new DBAppException("No such column");
                }
                Table t = tables.get(i);
                t.createRTreeIndex(strColName);

                serializeTable();
                break;
            }
        }
        if (!found) {
            throw new DBAppException("No such table");
        }
    }


}
