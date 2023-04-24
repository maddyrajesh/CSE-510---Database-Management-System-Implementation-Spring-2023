package tests;

import BigT.Map;
import BigT.Stream;
import BigT.bigt;
import diskmgr.pcounter;
import BigT.*;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import diskmgr.*;
import driver.*;
import global.AttrType;
import global.MID;
import global.SystemDefs;
import heap.*;

import java.io.*;
import java.util.Objects;

import static global.GlobalConst.NUMBUF;

import static global.GlobalConst.NUMBUF;

public class RowSortTest {

    private static final int NUM_PAGES = 100000;
    private static int mapCount = 0;

    //   This creates the databases that are used in the tests. These databases use the data from the given csv file
    // on Ed's Discussion.

    private static void createDatabase(String datbaseName, String datatableName, int type) {
        Integer numPages = NUM_PAGES;
        pcounter.initialize();

        FileInputStream fileStream = null;
        BufferedReader br = null;
        try {
            File f = new File(datbaseName);
            new SystemDefs(datbaseName, numPages, NUMBUF, "Clock");
            bigt database = new bigt(datatableName, type);
            fileStream = new FileInputStream("test_data1.csv");
            br = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"));
            String inputStr;
            while ((inputStr = br.readLine()) != null) {
                String[] input = inputStr.split(",");
                Map map = new Map();
                map.setHeader(BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES);
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[2]));
                map.setValue(input[3]);

                MID mid = database.insertMap(map.getMapByteArray());
                mapCount++;
            }
            br.close();
            fileStream.close();
            /*System.out.println("Index strategy: " + type);
            System.out.println("=======================================\n");
            System.out.println("map count: " + database.getMapCnt());
            System.out.println("Distinct Rows = " + database.getRowCnt());
            System.out.println("Distinct Coloumns = " + database.getColumnCnt());
            System.out.println("\n=======================================\n");
            System.out.println("Reads : " + pcounter.rcounter);
            System.out.println("Writes: " + pcounter.wcounter);
            System.out.println("NumBUFS: " + NUMBUF);
            System.out.println("\n=======================================\n");*/
            database.close();
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
            //database.deleteBigt();
        } catch (InvalidMapSizeException e) {
            e.printStackTrace();
        } catch (ConstructPageException e) {
            e.printStackTrace();
        } catch (AddFileEntryException e) {
            e.printStackTrace();
        } catch (InvalidStringSizeArrayException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (HFException e) {
            e.printStackTrace();
        } catch (GetFileEntryException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void test1() throws Exception {
        createDatabase("rowSortTest.db", "rowSortTest", 1);
        pcounter.initialize();
        new SystemDefs("rowSortTest.db", 0, NUMBUF, "Clock");
        bigt databaseTest = new bigt("rowSortTest");
        //assert (databaseTest.getMapCnt() == 2);
        long tmpTime = System.nanoTime();
        Stream stream = databaseTest.openStream(1, "*", "*", "*");
        bigt rsbigT = new bigt("rowSort1", 1);
        rowSort rS = new rowSort(stream, "Moose", NUMBUF);
        Map map = new Map();
        int mapCount = 0;
        do {
            map = rS.getNext();
            if (map != null) {
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                mapCount++;
                rsbigT.insertMap(map.getMapByteArray());
                //assert(Objects.equals(map.getRowLabel(), "Sweden"));
                //map.print();
                //System.out.println();
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                //System.out.println("found matching map! value is: " + map.getColumnLabel());
                //System.out.println("found matching map! value is: " + map.getTimeStamp());
                //System.out.println("found matching map! value is: " + map.getValue());
            }
        } while (map != null);
        rS.closeSort();
        rsbigT.close();
        bigt check = new bigt("rowSort1");

        Scan mapScan = check.getHeapFile().openScan();
        MID mid = new MID();
        Map map1 = mapScan.getNext(mid);
        while (map1 != null) {
           // map1.print();
           // System.out.println();
            map1 = mapScan.getNext(mid);


        }
        mapScan.closescan();


        long test1Time = System.nanoTime();
        test1Time -= tmpTime;
        databaseTest.close();
        rsbigT.close();
        //rsbigT.deleteBigt();
        //SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("total map count of test1: " + mapCount);
        System.out.println("time is: " + test1Time/1000000);
    }

    public static void test2() throws Exception {
        //createDatabase("rowSortTest.db", "rowSortTest", 1);
        pcounter.initialize();
        new SystemDefs("rowSortTest.db", 0, NUMBUF, "Clock");
        //bigt databaseTest = new bigt("rowSortTest");
        //assert (databaseTest.getMapCnt() == 2);
        long tmpTime = System.nanoTime();
        //Stream stream = databaseTest.openStream(1, "*", "*", "*");
        bigt rsbigT = new bigt("rowSort1");
        Stream str   = rsbigT.openStream(1, "*", "*", "*");
       /* rowSort rS = new rowSort(stream, "Moose", NUMBUF);
        Map map = new Map();
        int mapCount = 0;
        do {
            map = rS.getNext();
            if (map != null) {
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                mapCount++;
                rsbigT.insertMap(map.getMapByteArray());
                //assert(Objects.equals(map.getRowLabel(), "Sweden"));
                //map.print();
                //System.out.println();
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                //System.out.println("found matching map! value is: " + map.getColumnLabel());
                //System.out.println("found matching map! value is: " + map.getTimeStamp());
                //System.out.println("found matching map! value is: " + map.getValue());
            }
        } while (map != null);
        rS.closeSort();
*/
        int mapCount = 0;
        Scan mapScan = rsbigT.getHeapFile().openScan();
        MID mid = new MID();
        Map map1 = mapScan.getNext(mid);
        while (map1 != null) {
            map1.print();
            mapCount++;
            System.out.println();
            map1 = mapScan.getNext(mid);

        }
        mapScan.closescan();

        /*
        Map map = new Map();
        do {
            map = str.getNext();
            if (map != null) {
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                mapCount++;
                //rsbigT.insertMap(map.getMapByteArray());
                //assert(Objects.equals(map.getRowLabel(), "Sweden"));
                //map.print();
                //System.out.println();
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                //System.out.println("found matching map! value is: " + map.getColumnLabel());
                //System.out.println("found matching map! value is: " + map.getTimeStamp());
                //System.out.println("found matching map! value is: " + map.getValue());
            }
        } while (map != null);
        */



        long test1Time = System.nanoTime();
        test1Time -= tmpTime;
        //databaseTest.close();
        rsbigT.close();
        rsbigT.deleteBigt();
        //SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        //stream.closestream();
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("total map count of test1: " + mapCount);
        System.out.println("time is: " + test1Time);
    }

    public static void test3() throws Exception {
        pcounter.initialize();
        new SystemDefs("rowSortTest.db", 0, NUMBUF, "Clock");
        bigt databaseTest = new bigt("rowSortTest");
        //assert (databaseTest.getMapCnt() == 2);
        long tmpTime = System.nanoTime();
        Stream stream = databaseTest.openStream(1, "*", "*", "*");
        bigt rsbigT = new bigt("rowSort1", 1);
        rowSort rS = new rowSort(stream, "Moose", NUMBUF);
        Map map = new Map();
        int mapCount = 0;
        do {
            map = rS.getNext();
            if (map != null) {
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                mapCount++;
                rsbigT.insertMap(map.getMapByteArray());
                //assert(Objects.equals(map.getRowLabel(), "Sweden"));
                //map.print();
                //System.out.println();
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                //System.out.println("found matching map! value is: " + map.getColumnLabel());
                //System.out.println("found matching map! value is: " + map.getTimeStamp());
                //System.out.println("found matching map! value is: " + map.getValue());
            }
        } while (map != null);
        rS.closeSort();
        rsbigT.close();
        bigt check = new bigt("rowSort1");

        Scan mapScan = check.getHeapFile().openScan();
        MID mid = new MID();
        Map map1 = mapScan.getNext(mid);
        while (map1 != null) {
            // map1.print();
            // System.out.println();
            map1 = mapScan.getNext(mid);


        }
        mapScan.closescan();


        long test1Time = System.nanoTime();
        test1Time -= tmpTime;
        databaseTest.close();
        rsbigT.close();
        //rsbigT.deleteBigt();
        //SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("total map count of test1: " + mapCount);
        System.out.println("time is: " + test1Time/1000000);
    }

    public static void main(String [] args) throws Exception {
        test1();
        test2();
        test3();
    }
}
