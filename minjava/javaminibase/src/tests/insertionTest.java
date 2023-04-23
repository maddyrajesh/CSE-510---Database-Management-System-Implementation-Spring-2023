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

import java.io.IOException;
import java.util.Objects;

import static global.GlobalConst.NUMBUF;

public class insertionTest {
    private static final int NUM_PAGES = 100000;

    public static void test1() throws Exception {
        for(int i = 0; i < 10; i++)
            Utils.mapInsert("Sweden", "Moose" + String.valueOf(i), "testVal" + String.valueOf(i), String.valueOf(i), 2, "mapInsertTest", NUMBUF);
        Utils.mapInsert("Sweden", "Shark", "testVal2", "61341", 2, "mapInsertTest", NUMBUF);
        pcounter.initialize();
        new SystemDefs("mapInsertTest.db", 0, NUMBUF, "Clock");
        bigt databaseTest = new bigt("mapInsertTest");
        //assert (databaseTest.getMapCnt() == 2);
        long tmpTime = System.nanoTime();
        Stream stream = databaseTest.openStream(1, "Sweden", "*", "*");
        Map map = new Map();
        int mapCount = 0;
        do {
            map = stream.getNext();
            if (map != null) {
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                mapCount++;
                assert(Objects.equals(map.getRowLabel(), "Sweden"));
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                //System.out.println("found matching map! value is: " + map.getColumnLabel());
                //System.out.println("found matching map! value is: " + map.getTimeStamp());
                //System.out.println("found matching map! value is: " + map.getValue());
            }
        } while (map != null);
        //test1Time = System.nanoTime();
        //test1Time -= tmpTime;
        SystemDefs.JavabaseDB.closeDB();
        databaseTest.close();
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("total map count of test1: " + mapCount);
    }

    public static void test2() throws Exception {
        pcounter.initialize();
        new SystemDefs("mapInsertTest.db", 0, NUMBUF, "Clock");
        bigt databaseTest = new bigt("mapInsertTest");
        //assert (databaseTest.getMapCnt() == 2);
        long tmpTime = System.nanoTime();
        Stream stream = databaseTest.openStream(1, "Sweden", "*", "*");
        Map map = new Map();
        int mapCount = 0;
        do {
            map = stream.getNext();
            if (map != null) {
                mapCount++;
                assert(Objects.equals(map.getRowLabel(), "Sweden"));
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                //System.out.println("found matching map! value is: " + map.getColumnLabel());
                //System.out.println("found matching map! value is: " + map.getTimeStamp());
                //System.out.println("found matching map! value is: " + map.getValue());
            }
        } while (map != null);
        //test1Time = System.nanoTime();
        //test1Time -= tmpTime;
        SystemDefs.JavabaseDB.closeDB();
        databaseTest.close();
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("total map count of test1: " + mapCount);
    }

    public static void main(String [] args) throws Exception {
        test1();
        test2();
    }
}
