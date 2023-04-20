package BigT;

import btree.*;
import driver.BigTable;
import global.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import org.w3c.dom.Attr;

import java.io.File;
import java.io.IOException;


/**
 * The type Stream.
 */
public class Stream implements GlobalConst{
    /**
     * Instantiates a new Stream.
     *
     * @param bigtable     the bigtable
     * @param orderType    the order type
     * @param rowFilter    the row filter
     * @param columnFilter the column filter
     * @param valueFilter  the value filter
     */
    private bigt bigtable;
    private int orderType;
    private String rowFilter;
    private String columnFilter;
    private String valueFilter;
    private int tableType;
    private MapSort sort;
    private Scan scan;
    private boolean created = false;

    private Heapfile tempHeapFile;


    /**
     * Note that one record in our way-cool HeapFile implementation is
     * specified by six (6) parameters, some of which can be determined
     * from others:
     */

    /** The heapfile we are using. */
    private Heapfile  _hf;

    /** Add on (stephanie)*/
    private String rangeRegex = "\\[\\S+,\\S+]";
    private String starFilter = "*";
    private boolean scanAll = false;
    private String lastChar = "Z";
    private BTFileScan btreeScanner;
    /** Add on (stephanie)*/

    public Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        this.bigtable = bigtable;
        this.orderType = orderType;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        tableType = bigtable.getType();
        queryConditions(this.bigtable.getType());
        filterAndSortData(orderType);
    }

    /**
     * Closestream.
     */
    public void closestream() throws SortException {
        scanAll = false;
        btreeScanner = null;
        this.sort.close();
    }


    /** Retrieve the next record in the stream.
     *
     * @exception InvalidMapSizeException Invalid map size
     * @exception IOException I/O errors
     *
     * @param mid Record ID of the record
     * @return the Map of the retrieved record.
     */
    public Map getNext(MID mid)
            throws Exception {
        Map recptrmap = null;

        recptrmap = this.sort.get_next();
        mid = new MID();


        /*if (nextUserStatus != true) {
            nextDataPage();
        }

        if (datapage == null)
            return null;

        mid.pageNo.pid = usermid.pageNo.pid;
        mid.slotNo = usermid.slotNo;

        try {
            recptrmap = datapage.getMap(mid);
        }

        catch (Exception e) {
            //    System.err.println("SCAN: Error in Scan" + e);
            e.printStackTrace();
        }

        usermid = datapage.nextMap(mid);
        if(usermid == null) nextUserStatus = false;
        else nextUserStatus = true;
        */
        return recptrmap;
    }

    public Map getNext() throws Exception {
        Map map = this.sort.get_next();
        MID mid = new MID();
        if (map == null)
        {
            this.tempHeapFile.deleteFile();
            closestream();
            return null;
        }
        else
        {
            return map;
        }
    }


    public void queryConditions(int indexType) throws Exception {


        StringKey start = null, end = null;

        /*
        type is an integer denoting the different clustering and indexing strategies you will use for the graph database.
        orderType is for ordering by
        · 1, then results are first ordered in row label, then column label, then time stamp
        · 2, then results are first ordered in column label, then row label, then time stamp
        · 3, then results are first ordered in row label, then time stamp
        · 4, then results are first ordered in column label, then time stamp
        · 5, then results are ordered in time stamp
        * */
        switch (indexType) {
            default:
                // same as case 1
                this.scanAll = true;
                break;
            case 2:
                if (rowFilter.equals(starFilter)) {
                    this.scanAll = true;
                } else {
                    // check if range
                    if (rowFilter.matches(rangeRegex)) {
                        String[] range = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey(range[0]);
                        end = new StringKey(range[1] + this.lastChar);
                    } else {
                        start = new StringKey(rowFilter);
                        end = new StringKey(rowFilter + this.lastChar);
                    }
                }
                break;
            case 3:
                if (columnFilter.equals(starFilter)) {
                    this.scanAll = true;
                } else {
                    // check if range
                    if (columnFilter.matches(rangeRegex)) {
                        String[] range = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey(range[0]);
                        end = new StringKey(range[1] + this.lastChar);
                    } else {
                        start = new StringKey(columnFilter);
                        end = new StringKey(columnFilter + this.lastChar);
                    }
                }
                break;
            case 4:
                if ((rowFilter.equals(starFilter)) && (columnFilter.equals(starFilter))) {
                    scanAll = true;
                } else {

                    // check if both range
                    if ((rowFilter.matches(rangeRegex)) && (columnFilter.matches(rangeRegex))) {

                        String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        String[] columnRange = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey(rowRange[0]  + columnRange[0]);
                        end = new StringKey(rowRange[1]  + columnRange[1] + this.lastChar);

                        //check row range and column fixed/*
                    } else if ((rowFilter.matches(rangeRegex)) && (!columnFilter.matches(rangeRegex))) {
                        String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        if (columnFilter.equals(starFilter)) {
                            scanAll = true;
                        } else {
                            start = new StringKey(rowRange[0]  + columnFilter);
                            end = new StringKey(rowRange[1]  + columnFilter + this.lastChar);
                        }
                        // check column range and row fixed/*
                    } else if ((!rowFilter.matches(rangeRegex)) && (columnFilter.matches(rangeRegex))) {
                        String[] columnRange = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
                        if (rowFilter.equals(starFilter)) {
                            start = new StringKey(columnRange[0]);
                            end = new StringKey(columnRange[1] + this.lastChar);
                        } else {
                            start = new StringKey(rowFilter  + columnRange[0]);
                            end = new StringKey(rowFilter  + columnRange[1] + this.lastChar);
                        }

                        //row and col are fixed val or *,fixed fixed,*
                    } else {
                        if (columnFilter.equals(starFilter)) {
                            scanAll = true;
                        } else if (rowFilter.equals(starFilter)) {
                            start = new StringKey(columnFilter);
                            end = new StringKey(columnFilter + this.lastChar);
                        } else {
                            start = new StringKey(rowFilter  + columnFilter);
                            end = new StringKey(rowFilter + columnFilter + this.lastChar);
                        }
                    }
                }
                break;
            case 5:
                if ((valueFilter.equals(starFilter)) && (rowFilter.equals(starFilter))) {
                    scanAll = true;
                } else {

                    // check if both range
                    if ((valueFilter.matches(rangeRegex)) && (rowFilter.matches(rangeRegex))) {

                        String[] valueRange = valueFilter.replaceAll("[\\[ \\]]", "").split(",");
                        String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        start = new StringKey(rowRange[0]  + valueRange[0]);
                        end = new StringKey(rowRange[1]  + valueRange[1] + this.lastChar);

                        //check row range and column fixed/*
                    } else if ((valueFilter.matches(rangeRegex)) && (!rowFilter.matches(rangeRegex))) {
                        String[] valueRange = valueFilter.replaceAll("[\\[ \\]]", "").split(",");
                        if (rowFilter.equals(starFilter)) {
                            scanAll = true;
                        } else {
                            start = new StringKey(rowFilter  + valueRange[0]);
                            end = new StringKey(rowFilter + valueRange[1] + this.lastChar);
                        }
                        // check column range and row fixed/*
                    } else if ((!valueFilter.matches(rangeRegex)) && (rowFilter.matches(rangeRegex))) {
                        String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        if (valueFilter.equals("*")) {
                            start = new StringKey(rowRange[0]);
                            end = new StringKey(rowRange[1] + this.lastChar);
                        } else {

                            start = new StringKey(rowRange[0] + valueFilter);
                            end = new StringKey(rowRange[1] + valueFilter + this.lastChar);
                        }

                        //row and col are fixed val or *,fixed fixed,*
                    } else {
                        if (rowFilter.equals(starFilter)) {
                            // *, fixed
                            scanAll = true;
                        } else if (valueFilter.equals(starFilter)) {
                            // fixed, *
                            start = new StringKey(rowFilter);
                            end = new StringKey(rowFilter + lastChar);
                        } else {
                            // both fixed
                            start = new StringKey(rowFilter + valueFilter);
                            end = new StringKey(rowFilter + valueFilter + this.lastChar);
                        }
                    }
                }
        }

        if (!this.scanAll) {
            switch(indexType) {
                case 1:
                    this.btreeScanner = bigtable.btree1.new_scan(start, end);
                    break;
                case 2:
                    this.btreeScanner = bigtable.btree1.new_scan(start, end);
                    break;
                case 3:
                    this.btreeScanner = bigtable.btree1.new_scan(start, end);
                    break;
                case 4:
                    this.btreeScanner = bigtable.btree1.new_scan(start, end);
                    break;
                case 5:
                    this.btreeScanner = bigtable.btree1.new_scan(start, end);
                    break;
            }
        }


    }

    public void filterAndSortData(int orderType) throws Exception {
        /* orderType is for ordering by
        · 1, then results are first ordered in row label, then column label, then time stamp
        · 2, then results are first ordered in column label, then row label, then time stamp
        · 3, then results are first ordered in row label, then time stamp
        · 4, then results are first ordered in column label, then time stamp
        · 6, then results are ordered in time stamp
        * */

        String name = "tempSort " + bigtable.name + " " + bigtable.counter;
        this.tempHeapFile = new Heapfile(name);
        //scan = tempHeapFile.openScan();
        MID mid = new MID();

//            if (rowFilter.equals(starFilter) && columnFilter.equals(starFilter) && valueFilter.equals(starFilter)) {
//                System.out.println("rowFilter = " + rowFilter);
//                tempHeapFile = this.bigtable.heapfile;
//            } else {

        int count = 0;
        /*map = this.scan.getNext(mid);
        while (map != null) {
            count++;
            short kaka = 0;
            this.tempHeapFile.deleteMap(mid);
            map = scan.getNext(mid);
        }*/
        System.out.println(tempHeapFile._fileName + " " + tempHeapFile.getRecCnt());
        if(bigtable.counter >= Integer.MAX_VALUE)
            bigtable.counter = 0;
        else
            bigtable.counter++;
        //System.out.println("record before" + tempHeapFile.getRecCnt());


        if (this.scanAll) {
            //scanning whole bigt file.
            //System.out.println("scanning whole file");
            scan = bigtable.getHeapFile().openScan();

            //mapObj.setHeader();

//            if (rowFilter.equals(starFilter) && columnFilter.equals(starFilter) && valueFilter.equals(starFilter)) {
//                System.out.println("rowFilter = " + rowFilter);
//                tempHeapFile = this.bigtable.heapfile;
//            } else {

            //int count = 0;
            Map map = this.scan.getNext(mid);
            while (map != null) {
                count++;
                short kaka = 0;
                if (genericMatcher(map, "row", rowFilter) && genericMatcher(map, "column", columnFilter) && genericMatcher(map, "value", valueFilter)) {
                    this.tempHeapFile.insertMap(map.getMapByteArray());
                    //map.print();
                }
                map = scan.getNext(mid);
            }
            //System.out.println(tempHeapFile.getRecCnt());
            this.scan.closescan();

        } else {

            KeyDataEntry entry = btreeScanner.get_next();
            while (entry != null) {
                MID currMid = ((LeafData) entry.data).getData();
                if (currMid != null) {
                    MID tempMid = new MID(currMid.pageNo, currMid.slotNo);
                    Map map = bigtable.getHeapFile().getMap(tempMid);
                    if (genericMatcher(map, "row", rowFilter) && genericMatcher(map, "column", columnFilter) && genericMatcher(map, "value", valueFilter)) {
                        this.tempHeapFile.insertMap(map.getMapByteArray());
                    }

                }
                entry = btreeScanner.get_next();
            }
            //System.out.println("record count is " + tempHeapFile.getRecCnt());
            btreeScanner.DestroyBTreeFileScan();
        }


        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projection[0] = new FldSpec(rel, 1);
        projection[1] = new FldSpec(rel, 2);
        projection[2] = new FldSpec(rel, 3);
        projection[3] = new FldSpec(rel, 4);

        FileScan fscan = null;
        AttrType[] attrTypes = new AttrType[]{new AttrType(0), new AttrType(0), new AttrType(1), new AttrType(0)};
        //short strSizes[] = {}
        //String name = bigtable.name + ".heap";

        try {
            fscan = new FileScan(name, attrTypes, new short[]{(short) (32 * 1024 - 1), (short) (32 * 1024 - 1), (short) (32 * 1024 - 1)}, (short) 4, 4, projection, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        int sortField, num_pages = 10, sortFieldLength;
        switch (orderType) {
            case 1:
            case 3:
                sortField = 1;
                sortFieldLength = 32 * 1024 - 1;
                break;
            case 2:
            case 4:
                sortField = 2;
                sortFieldLength = 32 * 1024 - 1;
                break;
            case 5:
                sortField = 3;
                sortFieldLength = 32 * 1024 - 1;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + orderType);
        }
        try {
            this.sort = new MapSort(BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES, fscan, sortField, new MapOrder(MapOrder.Ascending), num_pages, sortFieldLength, orderType);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * @param map Map object to compare field with filter.
     * @param field field type to be compared - row, column or value.
     * @param filter Filter on fields - row, column or value.
     * @return
     * @throws Exception
     */
    private boolean genericMatcher(Map map, String field, String filter) throws Exception {
        if (filter.matches(rangeRegex)) {
            String[] range = filter.replaceAll("[\\[ \\]]", "").split(",");
//            String[] range = ",".split(genericFilter.replaceAll("[\\[ \\]]", ""));
            // so now row is in range
//            System.out.println("range = " + Arrays.toString(range));
            //System.out.println("in range regex check");
            switch (field)
            {
                case "row":
                    return map.getRowLabel().compareTo(range[0]) >= 0 && map.getRowLabel().compareTo(range[1]) <= 0;
                case "column":
                    return map.getColumnLabel().compareTo(range[0]) >= 0 && map.getColumnLabel().compareTo(range[1]) <= 0;
                case "value":
                    return map.getValue().compareTo(range[0]) >= 0 && map.getValue().compareTo(range[1]) <= 0;
            }

        } else if (filter.equals(map.getRowLabel()) || filter.equals(map.getColumnLabel()) || filter.equals(map.getValue())) {
            return true;
        } else {
            return filter.equals(starFilter);
        }
        return false;
    }
}

