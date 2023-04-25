package BigT;

import btree.*;
import bufmgr.BufMgrException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import driver.BigTable;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import driver.Utils;
import global.*;
import heap.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.MapUtils;
import iterator.RelSpec;

import java.io.*;
import java.util.*;

/**
 * The type Bigt.
 */
public class bigt {

    //private static final int MINIBASE_PAGESIZE = ;
    //public static final int MAX_SIZE = MINIBASE_PAGESIZE;
    public Heapfile[] heapfiles;
    String[] heapfileNames;
    String[] indexfileNames;
    // Name of the BigT file
    String name;
    BTreeFile[] indexFiles;

    public bigt(String name, boolean createNew) {

        this.name = name;
        try {
            Boolean tableExists;
            this.heapfileNames = new String[]{name + ".no.heap", name + ".row.heap", name + ".col.heap", name + ".col_row.heap", name + ".row_val.heap"};
            this.indexfileNames = new String[]{null, name + ".row.idx", name + ".col.idx", name + ".col_row.idx", name + ".row_val.idx"};
            PageId heapFilePageId = SystemDefs.JavabaseDB.get_file_entry(this.heapfileNames[0]);

            tableExists = heapFilePageId != null;


            if (!tableExists) {
                Utils.addTableToInventory(name);
                this.indexFiles = new BTreeFile[]{null, new BTreeFile(name + ".row.idx", AttrType.attrString, BigTable.BIGT_STR_SIZES[0], DeleteFashion.NAIVE_DELETE),
                        new BTreeFile(name + ".col.idx", AttrType.attrString, BigTable.BIGT_STR_SIZES[1], DeleteFashion.NAIVE_DELETE),
                        new BTreeFile(name + ".col_row.idx", AttrType.attrString, BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[1] + "$".getBytes().length, DeleteFashion.NAIVE_DELETE),
                        new BTreeFile(name + ".row_val.idx", AttrType.attrString, BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[2] + "$".getBytes().length, DeleteFashion.NAIVE_DELETE)};
            } else {
                this.indexFiles = new BTreeFile[]{null, new BTreeFile(name + ".row.idx"), new BTreeFile(name + ".col.idx"), new BTreeFile(name + ".col_row.idx"), new BTreeFile(name + ".row_val.idx")};
            }

            this.heapfiles = new Heapfile[]{new Heapfile(name + ".no.heap"), new Heapfile(name + ".row.heap"), new Heapfile(name + ".col.heap"), new Heapfile(name + ".col_row.heap"), new Heapfile(name + ".row_val.heap")};

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void createIndex(Integer type) throws Exception {
        switch (type) {
            default:
            case 1:
                break;
            // one btree to index row labels
            case 2:
                this.indexFiles[0] = new BTreeFile("rowIndex", AttrType.attrString, BigTable.BIGT_STR_SIZES[0], 0);
                break;
            // one btree to index column labels
            case 3:
                this.indexFiles[0] = new BTreeFile("columnIndex", AttrType.attrString, BigTable.BIGT_STR_SIZES[1], 0);
                break;
            /*
            one btree to index column label and row label (combined key)
             */
            case 4:
                this.indexFiles[0] = new BTreeFile("rowColumnIndex", AttrType.attrString, BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[1], 0);
                break;
            /*
            one btree to index row label and value (combined key)
             */
            case 5:
                this.indexFiles[0] = new BTreeFile("rowValueIndex", AttrType.attrString, BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[2], 0);
                break;
        }


        Scan scan = heapfiles[0].openScan();
        MID mid = new MID();

        //mapObj.setHeader();

//            if (rowFilter.equals(starFilter) && columnFilter.equals(starFilter) && valueFilter.equals(starFilter)) {
//                System.out.println("rowFilter = " + rowFilter);
//                tempHeapFile = this.bigtable.heapfile;
//            } else {

        int count = 0;
        Map map = scan.getNext(mid);
        while (map != null) {
            System.out.print("Inserting index of map: ");
            count++;
            map.print();
            System.out.println();
            insertIndex(mid, type);
            map = scan.getNext(mid);
        }
        System.out.println(count);
        scan.closescan();
    }


    /**
     * Insert index.
     *
     * @param mid the mid
     * @throws Exception the exception
     */
    public void insertIndex(MID mid, int type) throws Exception {
        Map map = heapfiles[type].getMap(mid);
        switch(type)
        {
            case 2:
                indexFiles[type].insert(new StringKey(map.getRowLabel()), mid);
                break;
            case 3:
                indexFiles[type].insert(new StringKey(map.getColumnLabel()), mid);
                break;
            case 4:
                indexFiles[type].insert(new StringKey(map.getRowLabel() + map.getColumnLabel()), mid);
                break;
            case 5:
                indexFiles[type].insert(new StringKey(map.getRowLabel() + map.getValue()), mid);
                break;
        }
    }


    public void close() throws PageUnpinnedException, PagePinnedException, PageNotFoundException, HashOperationException, BufMgrException, IOException, HashEntryNotFoundException, InvalidFrameNumberException, ReplacerException {
        for (int i = 0; i < 5; i++) {
            if (this.indexFiles[i] != null) {
                this.indexFiles[i].close();
            }
        }
    }

    /*public void batchInsert(Heapfile heapfile, int type) throws Exception {
        Set<Integer> deletedTypes = new HashSet<>();
        type -= 1;
        MID oldestMID = null;


        Scan mapScan = heapfile.openScan();
        MID mid = new MID();
        Map map = mapScan.getNext(mid);
        int count = 1;
        while (map != null) {
            int oldestTimestamp = Integer.MAX_VALUE;
            int oldestType = -1;
            int updateType = -1;
            MID updateMID = null;
            System.out.print("\r" + count);
            count += 1;
            java.util.Map<Integer, ArrayList<MID>> searchResults = searchForRecords(map);
            ArrayList<MID> arrayList = new ArrayList<>();
            searchResults.values().forEach(arrayList::addAll);

            if (arrayList.size() > 3) {
                throw new Exception("This list size cannot be greater than 3");
            }
            for (Integer key : searchResults.keySet()) {
                for (MID mid1 : searchResults.get(key)) {
                    Map map1 = this.heapfiles[key].getMap(mid1);
                    if (arrayList.size() == 3) {
                        if (map1.getTimeStamp() < oldestTimestamp) {
                            oldestMID = mid1;
                            oldestTimestamp = map1.getTimeStamp();
                            oldestType = key;
                        }
                    }
                    if (map1.getTimeStamp() == map.getTimeStamp()) {
                        updateMID = mid1;
                        updateType = key;
                    }
                }
            }
            if (oldestType != -1) {
                if (map.getTimeStamp() < oldestTimestamp) {
                    map = mapScan.getNext(mid);
                    continue;
                }
            }
            if (updateMID != null) {
                this.heapfiles[updateType].deleteMap(updateMID);
            } else {
                if (oldestType != -1) {
                    this.heapfiles[oldestType].deleteMap(oldestMID);
                    deletedTypes.add(oldestType);
                }
            }
            this.heapfiles[type].insertMap(map.getMapByteArray());
            map = mapScan.getNext(mid);
        }
        deletedTypes.add(type);
        /*for (int i : deletedTypes) {
            if (i != 0) {
                insertMapFile(i);
            }
        }*/
   //     mapScan.closescan();
   // }

    // Return number of maps in the bigtable.
    public int getMapCnt() throws HFBufMgrException, IOException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, InvalidMapSizeException {
        int count = 0;
        for (int i = 0; i < 5; i++) {
            count += this.heapfiles[i].getRecCnt();
        }
        return count;
    }


    public static String getDBPath(String tableName) {
        return tableName  + ".db";
    }

    // Return number of distinct row labels in the bigtable.
    public int getRowCnt() throws Exception {
        BigTable.orderType = 1;
        Stream stream = this.openStream(1, "*", "*", "*");
        Map map = stream.getNext();
        String oldRowKey = map.getRowLabel();
        int distinctRows = 0;
        while (map != null) {
            if (!map.getRowLabel().equals(oldRowKey)) distinctRows += 1;
            oldRowKey = map.getRowLabel();
            map = stream.getNext();
        }
        distinctRows += 1;
        stream.closestream();
        return distinctRows;
    }

    // Return number of distinct column labels in the bigtable.
    public int getColumnCnt() throws Exception {
        BigTable.orderType = 2;
        Stream stream = this.openStream(2, "*", "*", "*");
        Map map = stream.getNext();
        String oldColKey = map.getColumnLabel();
        int distinctCols = 0;
        while (map != null) {
            if (!map.getColumnLabel().equals(oldColKey)) distinctCols += 1;
            oldColKey = map.getColumnLabel();
            map = stream.getNext();
        }
        distinctCols += 1;
        stream.closestream();
        return distinctCols;
    }

    public void insertMap(byte[] mapPtr, int type) throws Exception {
        type -= 1;
        MID oldestMID = null;
        MID updateMID = null;
        int oldestType = -1;
        int updateType = -1;
        int oldestTimestamp = Integer.MAX_VALUE;
        Map map = new Map(mapPtr, 0);
        //map.setData(mapPtr);
        java.util.ArrayList<java.util.Map<Integer, MID>> searchResults = searchForRecords(map);
        //ArrayList<MID> arrayList = new ArrayList<>();
        //searchResults.values().forEach(arrayList::addAll);
        /*if (searchResults.size() > 3) {
            System.out.println();
            throw new Exception("This list size cannot be greater than 3");
        }*/
        if(searchResults.size() < 3) {
            MID mid = this.heapfiles[type].insertMap(mapPtr);
            insertIndex(mid, type);
            return;
        }
        for (int i = 0; i < searchResults.size(); i++) {
            //MID mid1 = searchResults.get(i).get(0);
            int key = (int) searchResults.get(i).keySet().toArray()[0];
            MID mid1 = searchResults.get(i).get(key);
            Map map1 = this.heapfiles[key].getMap(mid1);
            if (map1.getTimeStamp() < oldestTimestamp) {
                oldestMID = mid1;
                oldestTimestamp = map1.getTimeStamp();
                oldestType = key;
            }
            if (map1.getTimeStamp() == map.getTimeStamp()) {
                updateMID = mid1;
                updateType = key;
            }
        }

        if (map.getTimeStamp() < oldestTimestamp) {
            return;
        }
        //deleteIndex(updateMID, updateType);
            //this.heapfiles[updateType].deleteMap(updateMID);
        deleteIndex(oldestMID, oldestType);
        this.heapfiles[oldestType].deleteMap(oldestMID);
        //System.out.println("removing map: " + oldestMID);


        MID mid = this.heapfiles[type].insertMap(mapPtr);
        insertIndex(mid, type);
        return;

        /*if (oldestType != -1 && oldestType != 0) {
            insertMapFile(oldestType);
        }
        this.heapfiles[type].insertMap(mapPtr);
        if (type != 0) {
            insertMapFile(type);
        }*/

    }



    /*private void insertMapFile(int type) throws HFDiskMgrException, InvalidTupleSizeException, InvalidMapSizeException, IOException, InvalidSlotNumberException, SpaceNotAvailableException, HFException, HFBufMgrException {
        BigTable.insertType = type;
        MID mid = new MID();
        MapScan mapScan = this.heapfiles[type].openMapScan();
        Heapfile tempHeapFile = new Heapfile(String.format("%s.%d.tmp.heap", this.name, type));
        Map map1 = mapScan.getNext(mid);
        while (map1 != null) {
            tempHeapFile.insertMap(map1.getMapByteArray());
            map1 = mapScan.getNext(mid);
        }
        mapScan.closescan();
        FileScan fscan = null;
        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projection[0] = new FldSpec(rel, 1);
        projection[1] = new FldSpec(rel, 2);
        projection[2] = new FldSpec(rel, 3);
        projection[3] = new FldSpec(rel, 4);

        try {
            fscan = new FileScan(String.format("%s.%d.tmp.heap", this.name, type), BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES, (short) 4, 4, projection, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        int sortField, num_pages = 10, sortFieldLength;
        MapSort sortObj;
        switch (type) {
            case 1:
            case 4:
                sortField = 1;
                sortFieldLength = BigTable.BIGT_STR_SIZES[0];
                break;
            case 2:
            case 3:
                sortField = 2;
                sortFieldLength = BigTable.BIGT_STR_SIZES[1];
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        try {
            this.heapfiles[type].deleteFile();
            this.indexFiles[type].destroyFile();
            switch (type) {
                case 1:
                    this.indexFiles[type] = new BTreeFile(name + ".row.idx", AttrType.attrString, BigTable.BIGT_STR_SIZES[0], DeleteFashion.NAIVE_DELETE);
                    break;
                case 2:
                    this.indexFiles[type] = new BTreeFile(name + ".col.idx", AttrType.attrString, BigTable.BIGT_STR_SIZES[1], DeleteFashion.NAIVE_DELETE);
                    break;
                case 3:
                    this.indexFiles[type] = new BTreeFile(name + ".col_row.idx", AttrType.attrString, BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[1] + "$".getBytes().length, DeleteFashion.NAIVE_DELETE);
                    break;
                case 4:
                    this.indexFiles[type] = new BTreeFile(name + ".row_val.idx", AttrType.attrString, BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[2] + "$".getBytes().length, DeleteFashion.NAIVE_DELETE);
                    break;
                default:
                    throw new Exception("Undefined value");
            }
            this.heapfiles[type] = new Heapfile(this.heapfileNames[type]);
            sortObj = new MapSort(BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES, fscan, sortField, new MapOrder(MapOrder.Ascending), num_pages, sortFieldLength, true);
            Map map2 = sortObj.get_next();
            while (map2 != null) {
                MID mid1 = this.heapfiles[type].insertMap(map2.getMapByteArray());
                StringKey stringKey;
                switch (type) {
                    case 1:
                        stringKey = new StringKey(map2.getRowLabel());
                        break;
                    case 2:
                        stringKey = new StringKey(map2.getColumnLabel());
                        break;
                    case 3:
                        stringKey = new StringKey(map2.getColumnLabel() + "$" + map2.getRowLabel());
                        break;
                    case 4:
                        stringKey = new StringKey(map2.getRowLabel() + "$" + map2.getValue());
                        break;
                    default:
                        throw new Exception("undefined value");
                }
                this.indexFiles[type].insert(stringKey, mid1);
                map2 = sortObj.get_next();
            }
            assert fscan != null;
            fscan.close();
            sortObj.close();
            tempHeapFile.deleteFile();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/

    private void addToArrayList(Map newMap, Map oldMap, java.util.ArrayList<java.util.Map<Integer, MID>> searchResults, MID mid, int key) throws IOException {
        if (MapUtils.checkSameMap(newMap, oldMap)) {
            //MID tempMid = new MID();
            // tempMid.setSlotNo(mid.getSlotNo());
            //tempMid.setPageNo(mid.getPageNo());
            java.util.Map<Integer, MID> tmp = new HashMap<Integer, MID>();
            tmp.put(key, mid);
            searchResults.add(tmp);
        }
    }

    private java.util.ArrayList<java.util.Map<Integer, MID>> searchForRecords(Map newMap) throws Exception {
        java.util.ArrayList<java.util.Map<Integer, MID>> searchResults = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            Scan scan = this.heapfiles[i].openScan();
            MID mid = new MID();
            Map map = scan.getNext(mid);

            while (map != null) {
                addToArrayList(newMap, map, searchResults, mid, i);
                map = scan.getNext(mid);
            }
            scan.closescan();
        }

        return searchResults;
    }

    public Stream openStream(int orderType, java.lang.String rowFilter, java.lang.String columnFilter, java.
            lang.String valueFilter) throws Exception {
        return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
    }


    /**
     * Delete index.
     *
     * @param mid the mid
     * @throws Exception the exception
     */
    public void deleteIndex(MID mid, int type) throws Exception {
        Map map = heapfiles[type].getMap(mid);
        switch(type)
        {
            case 2:
                indexFiles[type].Delete(new StringKey(map.getRowLabel()), mid);
                break;
            case 3:
                indexFiles[type].Delete(new StringKey(map.getColumnLabel()), mid);
                break;
            case 4:
                indexFiles[type].Delete(new StringKey(map.getRowLabel() + map.getColumnLabel()), mid);
                break;
            case 5:
                indexFiles[type].Delete(new StringKey(map.getRowLabel() + map.getValue()), mid);
                break;
        }
    }
}
