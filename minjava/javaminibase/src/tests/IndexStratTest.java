package tests;

//   This test class, using the given csv data, tests the different indexing and clustering strategies.

import BigT.*;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import cmdline.MiniTable;
import diskmgr.*;
import global.AttrType;
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
        Integer numPages = NUM_PAGES;
        pcounter.initialize();

        FileInputStream fileStream = null;
        BufferedReader br = null;
        try {
            //System.out.println(System.getProperty("user.dir"));
            String basePath = System.getProperty("user.dir");
            /*System.out.println(System.getProperty("user.dir"));
            File f = new File(basePath + "/tmp/strat1.db");
            new SystemDefs(basePath + "/tmp/strat1.db", numPages, NUMBUF, "Clock");
            bigt database1 = new bigt("strat1.in", 1);


            f = new File(basePath + "/tmp/strat2.db");
            new SystemDefs(basePath + "/tmp/strat2.db", numPages, NUMBUF, "Clock");
            bigt database2 = new bigt("strat2.in", 2);


            f = new File(basePath + "/tmp/strat3.db");
            new SystemDefs(basePath + "/tmp/strat3.db", numPages, NUMBUF, "Clock");
            bigt database3 = new bigt("strat3.in", 3);


            f = new File(basePath + "/tmp/strat4.db");
            new SystemDefs(basePath + "/tmp/strat4.db", numPages, NUMBUF, "Clock");
            bigt database4 = new bigt("strat4.in", 4);

*/
            File f = new File(basePath + "/tmp/strat5.db");
            new SystemDefs(basePath + "/tmp/strat5.db", numPages, NUMBUF, "Clock");
            bigt database5 = new bigt("strat5", 5);
            fileStream = new FileInputStream("test_data1.csv");
            br = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"));
            String inputStr;
            int mapCount = 0;
            while ((inputStr = br.readLine()) != null) {
                String[] input = inputStr.split(",");
                //set the map
                System.out.println(input[0]);
                System.out.println(input[1]);
                System.out.println(input[2]);
                System.out.println(input[3]);
                Map map = new Map();
                short strSizes[] = {(short)input[0].length(), (short)input[1].length(), (short)input[3].length()};
                AttrType attr[] = {new AttrType(AttrType.attrString),
                        new AttrType(AttrType.attrString),
                        new AttrType(AttrType.attrInteger),
                        new AttrType(AttrType.attrString)};
                map.setHeader(attr, strSizes);
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[2]));
                map.setValue(input[3]);
                System.out.println("row: " + map.getRowLabel());
                System.out.println("column: " + map.getColumnLabel());
                System.out.println("time: " + map.getTimeStamp());
                //MID mid = database1.insertMap(map.getMapByteArray(), strSizes);
               // mid = database2.insertMap(map.getMapByteArray(), strSizes);
                //mid = database3.insertMap(map.getMapByteArray(), strSizes);
               // mid = database4.insertMap(map.getMapByteArray(), strSizes);
                MID mid = database5.insertMap(map.getMapByteArray());
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

    public static void test1() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException, InvalidTupleSizeException {
        assert(database1.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = database1.openStream(1, null, "Sweden", null);
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

    public static void test2() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException, InvalidTupleSizeException {
        assert(database2.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = database2.openStream(1, null, "Sweden", null);
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

    public static void test3() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException, InvalidTupleSizeException {
        assert(database3.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = database3.openStream(1, null, "Sweden", null);
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

    public static void test4() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException, InvalidTupleSizeException {
        assert(database4.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = database4.openStream(1, null, "Sweden", null);
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

    public static void test5() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException, InvalidTupleSizeException {
        assert(database5.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = database4.openStream(1, null, "Sweden", null);
        MID mid = new MID();
        Map map = new Map();
        do {
            map = stream.getNext(mid);
        }
        while(map != null);
        test5Time = System.nanoTime();
        test5Time -= tmpTime;
    }


    public static void main(String [] args) throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException, InvalidTupleSizeException {
        init();
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
