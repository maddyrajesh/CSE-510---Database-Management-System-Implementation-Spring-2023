package driver;

import BigT.InvalidStringSizeArrayException;
import BigT.Map;
import BigT.Stream;
import BigT.bigt;
import BigT.rowSort;
import BigT.rowJoin;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.*;
import diskmgr.pcounter;
import global.AttrOperator;
import global.AttrType;
import global.MID;
import global.SystemDefs;
import heap.*;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import java.io.*;

import static global.GlobalConst.NUMBUF;

public class Utils {

    private static final int NUM_PAGES = 100000;

    static void batchInsert(String dataFile, String tableName, int type) throws IOException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HashOperationException, SpaceNotAvailableException, FieldNumberOutOfBoundException, ConstructPageException, AddFileEntryException, HFDiskMgrException, HFException, GetFileEntryException, HFBufMgrException, InvalidTupleSizeException, InvalidSlotNumberException, InvalidTypeException {
        String dbPath = getDBPath(tableName);
        //System.out.println(dbPath);
        File f = new File(dbPath);
        bigt bigTable;
        if (f.exists()) {
            Integer numPages = NUM_PAGES;
            new SystemDefs(dbPath, numPages, NUMBUF, "Clock");
            bigTable = new bigt(tableName, type);
        } else {
            new SystemDefs(dbPath, 0, NUMBUF, "Clock");
            bigTable = new bigt(tableName);
        }
        pcounter.initialize();

        FileInputStream fileStream = null;
        BufferedReader br = null;
        try {
            fileStream = new FileInputStream(dataFile);
            br = new BufferedReader(new InputStreamReader(fileStream));
            String inputStr;
            int mapCount = 0;

            while ((inputStr = br.readLine()) != null) {
                String[] input = inputStr.split(",");
                //set the map
                Map map = new Map();
                map.setHeader(BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES);
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[2]));
                map.setValue(input[3]);
                MID mid = bigTable.insertMap(map.getMapByteArray());
                mapCount++;
            }
            System.out.println("=======================================\n");
            System.out.println("map count: " + bigTable.getMapCnt());
            System.out.println("Distinct Rows = " + bigTable.getRowCnt());
            System.out.println("Distinct Columns = " + bigTable.getColumnCnt());
            System.out.println("\n=======================================\n");
            System.out.println("Reads : " + pcounter.rcounter);
            System.out.println("Writes: " + pcounter.wcounter);
            System.out.println("NumBUFS: " + NUMBUF);
            System.out.println("\n=======================================\n");
            bigTable.close();


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileStream.close();
            br.close();
        }

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
    }


    static void query(String tableName, Integer type, Integer orderType, String rowFilter, String colFilter, String valFilter, Integer NUMBUF) throws Exception {
        //String dbPath = getDBPath(tableName, type);
        String dbPath = getDBPath(tableName);
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        pcounter.initialize();
        int resultCount = 0;

        try {

            bigt bigTable = new bigt(tableName);
            if (!type.equals(bigTable.getType())) {
                System.out.println("Type Mismatch");
                bigTable.close();
                return;
            }
            Stream mapStream = bigTable.openStream(orderType, rowFilter, colFilter, valFilter);
            MID mapId = null;

             while (true) {
                 Map mapObj = mapStream.getNext();
                 if (mapObj == null)
                     break;
                 mapObj.print();
                 resultCount++;
             }
             bigTable.close();
             mapStream.closestream();

        } catch (Exception e) {
            e.printStackTrace();
        }
    
        System.out.println("\n=======================================\n");
        System.out.println("Matched Records: " + resultCount);
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("\n=======================================\n");
        
    }


    public static void mapInsert(String rowLabel, String colLabel, String value, String timestamp, Integer type, String tableName, Integer NUMBUF) throws Exception {
        String dbPath = getDBPath(tableName);
        Map map = new Map();
        map.setHeader(BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES);
        map.setRowLabel(rowLabel);
        map.setColumnLabel(colLabel);
        map.setTimeStamp(Integer.parseInt(timestamp));
        map.setValue(value);
        File f = new File(dbPath);
        if(f.exists()) {
            //System.out.println("already exist.\n");
            new SystemDefs(dbPath, 0, NUMBUF, "Clock");
            bigt bigTable = new bigt(tableName);
            if (!type.equals(bigTable.getType())) {
                System.out.println("Type Mismatch");
                bigTable.close();
                return;
            }
            MID mid = bigTable.insertMap(map.getMapByteArray());
            bigTable.close();
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        } else {
            //System.out.println("creating database");
            //f.createNewFile();
            new SystemDefs(dbPath, NUM_PAGES, NUMBUF, "Clock");
            bigt bigTable = new bigt(tableName, type);
            MID mid = bigTable.insertMap(map.getMapByteArray());
            bigTable.close();
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        }

    }


    public static void createIndex(String tableName, Integer type) throws Exception {
        String dbPath = getDBPath(tableName);
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        bigt bigTable = new bigt(tableName);
        bigTable.createIndex(type);
        bigTable.close();
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
    }


    public static void rowJoin(String tableName1, String tableName2, String outputTableName, String colFilter, String joinType, Integer NUMBUF) throws Exception {
        rowJoin rj;
        new SystemDefs(Utils.getDBPath(tableName1), Utils.NUM_PAGES, NUMBUF, "Clock");
        pcounter.initialize();

        Stream leftstream = new bigt(tableName1, 1).openStream(1, "*", colFilter, "*");
        rj = new rowJoin(20, leftstream, tableName2, colFilter, joinType);

        System.out.println("Query results => ");
        Utils.query(outputTableName, 1, 1,"*", "*", "*", NUMBUF);

        System.out.println("\n=======================================\n");
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("\n=======================================\n");

        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
    }


    public static void rowSort(String inputTableName, String outputTableName, String columnName, Integer NUMBUF) throws Exception {
        String dbPath = getDBPath(inputTableName);
        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        pcounter.initialize();

        bigt bigtable = new bigt(inputTableName, 1);
        Stream tempStream = bigtable.openStream(1, "*", "*", "*");
        bigt bigTable = new bigt( outputTableName, 1);
        rowSort rowSort = new rowSort(tempStream, columnName, NUMBUF);
        Map map = rowSort.getNext();
        int counter = 0;
        while (map != null) {
//            map.print();
            bigTable.insertMap(map.getMapByteArray());
            map = rowSort.getNext();
            counter++;
        }

        System.out.println("\n=======================================\n");
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("Map count: " + counter);
        System.out.println("\n=======================================\n");

        bigTable.close();
        rowSort.closeSort();
        //rowSort.closeStream();

        // Print Final results
//        new SystemDefs(dbPath, 0, NUMBUF, "Clock");
        System.out.println("Row Sort results=>");
        bigt result = new bigt(outputTableName);
        Scan mapScan = result.getHeapFile().openScan();
        MID mid = new MID();
        Map map1 = mapScan.getNext(mid);
        while (map1 != null) {
            map1.print();
            System.out.println();
            map1 = mapScan.getNext(mid);

        }
        //SystemDefs.JavabaseBM.flushAllPages();
        result.deleteBigt();
        SystemDefs.JavabaseDB.closeDB();
    }


    public static String getDBPath(String tableName) {
        return tableName  + ".db";
    }


    static CondExpr[] getCondExpr(String filter) {
        if (filter.equals("*")) {
            return null;
        } else if (filter.contains(",")) {
            String[] range = filter.replaceAll("[\\[ \\]]", "").split(",");
            //cond expr of size 3 for range searches
            CondExpr[] expr = new CondExpr[3];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopGE);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            expr[0].operand2.string = range[0];
            expr[0].next = null;
            expr[1] = new CondExpr();
            expr[1].op = new AttrOperator(AttrOperator.aopLE);
            expr[1].type1 = new AttrType(AttrType.attrSymbol);
            expr[1].type2 = new AttrType(AttrType.attrString);
            expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            expr[1].operand2.string = range[1];
            expr[1].next = null;
            expr[2] = null;
            return expr;
        } else {
            //equality search
            CondExpr[] expr = new CondExpr[2];
            expr[0] = new CondExpr();
            expr[0].op = new AttrOperator(AttrOperator.aopEQ);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            expr[0].operand2.string = filter;
            expr[0].next = null;
            expr[1] = null;
            return expr;
        }
    }
}


