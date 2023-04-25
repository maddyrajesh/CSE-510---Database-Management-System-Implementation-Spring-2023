package tests;

//   This test class, using the given csv data, tests the different indexing and clustering strategies.

import BigT.*;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import diskmgr.*;
import driver.BigTable;
import global.AttrType;
import global.MID;
import global.SystemDefs;
import heap.*;

import java.io.*;

import static global.GlobalConst.NUMBUF;

public class IndexStratTest {

    private static boolean createFlag = true;
    private static final int NUM_PAGES = 100000;
    private static int mapCount = 0;

    // These are the private timer variables that are used in each test function.

    private static long test1Time = 0, mapCount1 = 0;
    private static long test2Time = 0, mapCount2 = 0;
    private static long test3Time = 0, mapCount3 = 0;
    private static long test4Time = 0, mapCount4 = 0;
    private static long test5Time = 0, mapCount5 = 0;


    //   This creates the databases that are used in the tests. These databases use the data from the given csv file
    // on Ed's Discussion.

    private static void createDatabase(String datbaseName, String datatableName, int type) {
        Integer numPages = NUM_PAGES;
        pcounter.initialize();

        FileInputStream fileStream = null;
        BufferedReader br = null;
        int i = 1;
        try {
            File f = new File(datbaseName);
            new SystemDefs(datbaseName, numPages, NUMBUF, "Clock");
            bigt database = new bigt(datatableName, true);
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

                database.insertMap(map.getMapByteArray(), i);
                i++;
                i %= 6;
                if(i == 0)
                    i = 1;
                mapCount++;
            }
            br.close();
            fileStream.close();
            System.out.println("Index strategy: " + type);
            System.out.println("=======================================\n");
            System.out.println("map count: " + database.getMapCnt());
            System.out.println("Distinct Rows = " + database.getRowCnt());
            System.out.println("Distinct Coloumns = " + database.getColumnCnt());
            System.out.println("\n=======================================\n");
            System.out.println("Reads : " + pcounter.rcounter);
            System.out.println("Writes: " + pcounter.wcounter);
            System.out.println("NumBUFS: " + NUMBUF);
            System.out.println("\n=======================================\n");
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


    //  Test index strategy 1.

    public static void test1() throws Exception {
        createDatabase("strat1.db", "strat1", 1);
        pcounter.initialize();
        new SystemDefs("strat1.db", 0, NUMBUF, "Clock");
        //new SystemDefs("index1", 0, NUMBUF, "Clock");
        bigt databaseTest = new bigt("strat1", false);
        assert(databaseTest.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = databaseTest.openStream(1, "Switzerla", "*", "[00000,03000]");
        Map map = new Map();
        do {
            map = stream.getNext();
            if(map != null) {
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                mapCount1++;
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                //System.out.println("found matching map! value is: " + map.getColumnLabel());
                //System.out.println("found matching map! value is: " + map.getTimeStamp());
                //System.out.println("found matching map! value is: " + map.getValue());
            }
        } while(map != null);
        databaseTest.close();
        SystemDefs.JavabaseDB.closeDB();
        //databaseTest.deleteBigt();
        test1Time = System.nanoTime();
        test1Time -= tmpTime;
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("total map count of test1: " + mapCount1);
        System.out.println();
        mapCount1 = 0;
    }


    //  Test index strategy 2.

    public static void test2() throws Exception {
        if(createFlag) {
            //System.out.println("creating database.\n");
            createDatabase("strat2.db", "strat2", 2);
        }

        new SystemDefs("strat2.db", 0, NUMBUF, "Clock");
        pcounter.initialize();
        bigt databaseTest = new bigt("strat2", false);
        assert(databaseTest.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = databaseTest.openStream(2, "Sweden", "*", "*");
        MID mid = new MID();
        Map map = new Map();
        do {
            map = stream.getNext();
            if(map != null) {
                mapCount2++;
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
            }
        }
        while(map != null);
        databaseTest.close();
        stream.closestream();
        //SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        test2Time = System.nanoTime();
        test2Time -= tmpTime;
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("total map count of test2: " + mapCount2);
        System.out.println();
        mapCount2 = 0;
        createFlag = false;


        /*new SystemDefs("strat2.db", 0, NUMBUF, "Clock");
        bigt databaseTest2 = new bigt("strat2");
        assert(databaseTest2.getMapCnt() == mapCount);
        tmpTime = System.nanoTime();
        Stream stream2 = databaseTest2.openStream(1, "Sweden", "*", "*");
        map = new Map();
        do {
            map = stream2.getNext();
            if(map != null) {
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                mapCount2++;
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
                //System.out.println("found matching map! value is: " + map.getColumnLabel());
                //System.out.println("found matching map! value is: " + map.getTimeStamp());
                //System.out.println("found matching map! value is: " + map.getValue());
            }
        } while(map != null);
        databaseTest2.close();
        stream2.closestream();
        //SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        createFlag = false;
        //databaseTest.deleteBigt();

        test1Time = System.nanoTime();
        test1Time -= tmpTime;
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("total map count of test2: " + mapCount2);
        System.out.println();
        mapCount2 = 0;*/
    }


    //  Test index strategy 3.

    public static void test3() throws Exception {
        createDatabase("strat3.db", "strat3", 3);
        pcounter.initialize();
        new SystemDefs("strat3.db", 0, NUMBUF, "Clock");
        bigt databaseTest = new bigt("strat3", false);
        assert(databaseTest.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = databaseTest.openStream(1, "*", "Moose", "*");
        MID mid = new MID();
        Map map = new Map();
        do {
            map = stream.getNext();
            if(map != null) {
                mapCount3++;
                //System.out.println("found matching map! row is: " + map.getRowLabel() + " column is: " + map.getColumnLabel()  + " time is: " + map.getTimeStamp() + " val is: " + map.getValue());
            }
        }
        while(map != null);
        databaseTest.close();
        //SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        test3Time = System.nanoTime();
        test3Time -= tmpTime;
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("total map count of test3: " + mapCount3);
    }


    //  Test index strategy 4.

    public static void test4() throws Exception {
        //createDatabase("strat4.db", "strat4", 4);
        pcounter.initialize();
        new SystemDefs("strat4.db", 0, NUMBUF, "Clock");
        bigt databaseTest = new bigt("strat4", false);
        assert(databaseTest.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = databaseTest.openStream(1, "Sweden", "*", "*");
        MID mid = new MID();
        Map map = new Map();
        do {
            map = stream.getNext();
            if(map != null) {
                mapCount4++;
                //map.print();
                //System.out.println();
            }
        }
        while(map != null);
        test4Time = System.nanoTime();
        test4Time -= tmpTime;
        databaseTest.close();
        //SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("total map count of test4: " + mapCount4);
    }


    //  Test index strategy 5.

    public static void test5() throws Exception {
        //createDatabase("dhowa.db", "strat5", 5);
        pcounter.initialize();
        new SystemDefs("dhowa.db", 0, NUMBUF, "Clock");
        bigt databaseTest = new bigt("strat5", false);
        assert(databaseTest.getMapCnt() == mapCount);
        long tmpTime = System.nanoTime();
        Stream stream = databaseTest.openStream(1, "Sweden", "Moose", "*");
        MID mid = new MID();
        Map map = new Map();
        do {
            map = stream.getNext();
            if(map != null) {
                mapCount5++;
                //System.out.println("found matching map! value is: " + map.getValue());
            }
        }
        while(map != null);
        test5Time = System.nanoTime();
        test5Time -= tmpTime;
        databaseTest.close();
        //SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        System.out.println("Reads : " + pcounter.rcounter);
        System.out.println("Writes: " + pcounter.wcounter);
        System.out.println("total map count of test5: " + mapCount5);
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
        //test1();
        //test1();
        //test2();
       // test2();
       // test2();
        //test2();
        //test1();*/
        //test2();
        //test3();
        test4();
        //test5();
        //test2();
        System.out.println("Total time in milliseconds for query using strategy 1: " + test1Time/1_000_000);
        System.out.println("Total time in milliseconds for query using strategy 2: " + test2Time/1_000_000);
        System.out.println("Total time in milliseconds for query using strategy 3: " + test3Time/1_000_000);
        System.out.println("Total time in milliseconds for query using strategy 4: " + test4Time/1_000_000);
        System.out.println("Total time in milliseconds for query using strategy 5: " + test5Time/1_000_000);
    }
}
