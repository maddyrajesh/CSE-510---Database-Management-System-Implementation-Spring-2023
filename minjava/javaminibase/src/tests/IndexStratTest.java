package tests;

//   This test class, using the given csv data, tests the different indexing and clustering strategies.

import BigT.*;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import cmdline.MiniTable;
import diskmgr.*;
import global.MID;
import global.SystemDefs;
import heap.*;

import java.io.*;

import static global.GlobalConst.NUMBUF;

public class IndexStratTest {

    private static final int NUM_PAGES = 100000;
    private static int mapCount = 0;
    private static long test1Time = 0;
    private static long test2Time = 0;
    private static long test3Time = 0;
    private static long test4Time = 0;
    private static long test5Time = 0;

    //   Create the five different databases, each of which will have a different index/clustering strategy and will be
    // used in the comparison.

    private static bigt database1;
    private static bigt database2;
    private static bigt database3;
    private static bigt database4;
    private static bigt database5;


    private static void init() {
        File f = new File("test_data1.csv");
        Integer numPages = NUM_PAGES;
        new SystemDefs("test_data1.csv", numPages, NUMBUF, "Clock");
        pcounter.initialize();

        FileInputStream fileStream = null;
        BufferedReader br = null;
        try {
            bigt database1 = new bigt("strat1", 1);
            bigt database2 = new bigt("strat2", 2);
            bigt database3 = new bigt("strat3", 3);
            bigt database4 = new bigt("strat4", 4);
            bigt database5 = new bigt("strat5", 5);
            fileStream = new FileInputStream("test_data1.csv");
            br = new BufferedReader(new InputStreamReader(fileStream));
            String inputStr;
            int mapCount = 0;

            while ((inputStr = br.readLine()) != null) {
                String[] input = inputStr.split(",");
                //set the map
                Map map = new Map();
                map.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[2]));
                map.setValue(input[3]);
                MID mid = database1.insertMap(map.getMapByteArray());
                mid = database2.insertMap(map.getMapByteArray());
                mid = database3.insertMap(map.getMapByteArray());
                mid = database4.insertMap(map.getMapByteArray());
                mid = database5.insertMap(map.getMapByteArray());
                mapCount++;
            }
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


    //  Test index strategy 1.

    public static void test1() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException {
        assert(database1.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = database1.openStream(1, null, "Denmark", null);
        MID mid = new MID();
        Map map = new Map();
        do {
            map = stream.getNext(mid);
        }
        while(map != null);
        test1Time = System.nanoTime();
        test1Time -= tmpTime;
    }


    //  Test index strategy 2.

    public static void test2() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException {
        assert(database2.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = database2.openStream(1, null, "Denmark", null);
        MID mid = new MID();
        Map map = new Map();
        do {
            map = stream.getNext(mid);
        }
        while(map != null);
        test2Time = System.nanoTime();
        test2Time -= tmpTime;
    }


    //  Test index strategy 3.

    public static void test3() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException {
        assert(database3.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = database3.openStream(1, null, "Denmark", null);
        MID mid = new MID();
        Map map = new Map();
        do {
            map = stream.getNext(mid);
        }
        while(map != null);
        test3Time = System.nanoTime();
        test3Time -= tmpTime;
    }


    //  Test index strategy 4.

    public static void test4() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException {
        assert(database4.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = database4.openStream(1, null, "Denmark", null);
        MID mid = new MID();
        Map map = new Map();
        do {
            map = stream.getNext(mid);
        }
        while(map != null);
        test4Time = System.nanoTime();
        test4Time -= tmpTime;
    }


    //  Test index strategy 5.

    public static void test5() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException {
        assert(database5.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = database4.openStream(1, null, "Denmark", null);
        MID mid = new MID();
        Map map = new Map();
        do {
            map = stream.getNext(mid);
        }
        while(map != null);
        test5Time = System.nanoTime();
        test5Time -= tmpTime;
    }


    public static void main(String [] args) throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException {
        test1();
        System.out.print("test 1 time: " + test1Time);

        test2();
        System.out.print("test 2 time: " + test2Time);

        test3();
        System.out.print("test 3 time: " + test3Time);

        test4();
        System.out.print("test 4 time: " + test4Time);

        test5();
        System.out.print("test 5 time: " + test5Time);

    }
}
