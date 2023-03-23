package tests;

//   This test class, using the given csv data, tests the different indexing and clustering strategies.

import BigT.*;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
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

    // These are the private timer variables that are used in each test function.

    private static long test1Time = 0;
    private static long test2Time = 0;
    private static long test3Time = 0;
    private static long test4Time = 0;
    private static long test5Time = 0;


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
            bigt database1 = new bigt(datatableName, type);
            fileStream = new FileInputStream("test_data1.csv");
            br = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"));
            String inputStr;
            while ((inputStr = br.readLine()) != null) {
                String[] input = inputStr.split(",");
                Map map = new Map();
                short strSizes[] = setBigTConstants("test_data1.csv");
                AttrType attr[] = {new AttrType(AttrType.attrString),
                        new AttrType(AttrType.attrString),
                        new AttrType(AttrType.attrInteger),
                        new AttrType(AttrType.attrString)};
                map.setHeader(attr, strSizes);
                map.setRowLabel(input[0]);
                map.setColumnLabel(input[1]);
                map.setTimeStamp(Integer.parseInt(input[2]));
                map.setValue(input[3]);

                MID mid = database1.insertMap(map.getMapByteArray());
                mapCount++;
            }
            System.out.println("Index strategy: " + type);
            System.out.println("=======================================\n");
            System.out.println("map count: " + database1.getMapCnt());
            System.out.println("Distinct Rows = " + database1.getRowCnt());
            System.out.println("Distinct Coloumns = " + database1.getColumnCnt());
            System.out.println("\n=======================================\n");
            System.out.println("Reads : " + pcounter.rcounter);
            System.out.println("Writes: " + pcounter.wcounter);
            System.out.println("NumBUFS: " + NUMBUF);
            System.out.println("\n=======================================\n");
            database1.close();
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

    public static void test1() throws Exception {
        pcounter.initialize();
        long tmpTime = System.nanoTime();
        createDatabase("strat1.db", "index1", 1);
        bigt databaseTest = new bigt("index1", 1);
        assert(databaseTest.getMapCnt() == mapCount);
        test1Time = System.nanoTime();
        test1Time -= tmpTime;
        /*Stream stream = databaseTest.openStream(3, "Sweden", "Moose", "00002");
        MID mid = new MID();
        Map map = new Map();
        int numOfMatches = 0 ;
        do {
            map = stream.getNext(mid);
            if(map != null) {
                numOfMatches++;
                System.out.println(map.getColumnLabel());
            }
        }
        while(map != null);
        */
    }


    //  Test index strategy 2.

    public static void test2() throws Exception {
        pcounter.initialize();
        long tmpTime = System.nanoTime();
        createDatabase("strat2.db", "index2", 2);
        bigt databaseTest = new bigt("index2", 2);
        assert(databaseTest.getMapCnt() == mapCount);
        test2Time = System.nanoTime();
        test2Time -= tmpTime;
        /*Stream stream = databaseTest.openStream(3, "Sweden", "Moose", "00002");
        MID mid = new MID();
        Map map = new Map();
        int numOfMatches = 0 ;
        do {
            map = stream.getNext(mid);
            if(map != null) {
                numOfMatches++;
                System.out.println(map.getColumnLabel());
            }
        }
        while(map != null);
        */
    }


    //  Test index strategy 3.

    public static void test3() throws Exception {
        pcounter.initialize();
        long tmpTime = System.nanoTime();
        createDatabase("strat3.db", "index3", 3);
        bigt databaseTest = new bigt("index3", 3);
        assert(databaseTest.getMapCnt() == mapCount);
        test3Time = System.nanoTime();
        test3Time -= tmpTime;
        /*Stream stream = databaseTest.openStream(3, "Sweden", "Moose", "00002");
        MID mid = new MID();
        Map map = new Map();
        int numOfMatches = 0 ;
        do {
            map = stream.getNext(mid);
            if(map != null) {
                numOfMatches++;
                System.out.println(map.getColumnLabel());
            }
        }
        while(map != null);
        */
    }


    //  Test index strategy 4.

    public static void test4() throws Exception {
        pcounter.initialize();
        long tmpTime = System.nanoTime();
        createDatabase("strat4.db", "index4", 4);
        bigt databaseTest = new bigt("index4", 4);
        assert(databaseTest.getMapCnt() == mapCount);
        test4Time = System.nanoTime();
        test4Time -= tmpTime;
        /*Stream stream = databaseTest.openStream(3, "Sweden", "Moose", "00002");
        MID mid = new MID();
        Map map = new Map();
        int numOfMatches = 0 ;
        do {
            map = stream.getNext(mid);
            if(map != null) {
                numOfMatches++;
                System.out.println(map.getColumnLabel());
            }
        }
        while(map != null);
        */
    }


    //  Test index strategy 5.

    public static void test5() throws Exception {
        pcounter.initialize();
        long tmpTime = System.nanoTime();
        createDatabase("strat5.db", "index5", 5);
        bigt databaseTest = new bigt("index5", 5);
        assert(databaseTest.getMapCnt() == mapCount);
        test5Time = System.nanoTime();
        test5Time -= tmpTime;
        /*Stream stream = databaseTest.openStream(3, "Sweden", "Moose", "00002");
        MID mid = new MID();
        Map map = new Map();
        int numOfMatches = 0 ;
        do {
            map = stream.getNext(mid);
            if(map != null) {
                numOfMatches++;
                System.out.println(map.getColumnLabel());
            }
        }
        while(map != null);
        */
    }


    // Setting the map header constants. Copied from Minitable.java.

    private static short[] setBigTConstants(String dataFileName) {
        try (BufferedReader br = new BufferedReader(new FileReader(dataFileName))) {
            String line;
            int maxRowKeyLength = Short.MIN_VALUE;
            int maxColumnKeyLength = Short.MIN_VALUE;
            int maxValueLength = Short.MIN_VALUE;
            int maxTimeStampLength = Short.MIN_VALUE;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                OutputStream out = new ByteArrayOutputStream();
                DataOutputStream rowStream = new DataOutputStream(out);
                DataOutputStream columnStream = new DataOutputStream(out);
                DataOutputStream timestampStream = new DataOutputStream(out);
                DataOutputStream valueStream = new DataOutputStream(out);

                rowStream.writeUTF(fields[0]);
                maxRowKeyLength = Math.max(rowStream.size(), maxRowKeyLength);

                columnStream.writeUTF(fields[1]);
                maxColumnKeyLength = Math.max(columnStream.size(), maxColumnKeyLength);

                timestampStream.writeUTF(fields[2]);
                maxTimeStampLength = Math.max(timestampStream.size(), maxTimeStampLength);

                valueStream.writeUTF(fields[3]);
                maxValueLength = Math.max(valueStream.size(), maxValueLength);

            }
            return new short[]{
                    (short) maxRowKeyLength,
                    (short) maxColumnKeyLength,
                    (short) maxValueLength
            };
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return new short[0];
    }


    public static void main(String [] args) throws Exception {
        test1();
        test2();
        test3();
        test4();
        test5();
        System.out.println("Total time in milliseconds for database creation and batch insertion using strategy 1: " + test1Time/1_000_000);
        System.out.println("Total time in milliseconds for database creation and batch insertion using strategy 2: " + test2Time/1_000_000);
        System.out.println("Total time in milliseconds for database creation and batch insertion using strategy 3: " + test3Time/1_000_000);
        System.out.println("Total time in milliseconds for database creation and batch insertion using strategy 4: " + test4Time/1_000_000);
        System.out.println("Total time in milliseconds for database creation and batch insertion using strategy 5: " + test5Time/1_000_000);
    }
}
