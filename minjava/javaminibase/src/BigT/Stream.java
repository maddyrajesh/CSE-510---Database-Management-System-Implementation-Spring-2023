package BigT;

import btree.*;
import global.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import org.w3c.dom.Attr;

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


    /**
     * Note that one record in our way-cool HeapFile implementation is
     * specified by six (6) parameters, some of which can be determined
     * from others:
     */

    /** The heapfile we are using. */
    private Heapfile  _hf;

    /** PageId of current directory page (which is itself an HFPage) */
    private PageId dirpageId = new PageId();

    /** pointer to in-core data of dirpageId (page is pinned) */
    private HFPage dirpage = new HFPage();

    /** record ID of the DataPageInfo struct (in the directory page) which
     * describes the data page where our current record lives.
     */
    private MID datapageMid = new MID();

    /** the actual PageId of the data page with the current record */
    private PageId datapageId = new PageId();

    /** in-core copy (pinned) of the same */
    private HFPage datapage = new HFPage();

    /** record ID of the current record (from the current data page) */
    private MID usermid = new MID();

    /** Status of next user status */
    private boolean nextUserStatus;

    /** Add on (stephanie)*/
    private String rangeRegex = "\\[\\S+,\\S+\\]";
    private String starFilter = "*";
    private boolean scanAll = false;
    private String lastChar = "Z";
    private BTFileScan btreeScanner;
    /** Add on (stephanie)*/

    public Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) {
        this.bigtable = bigtable;
        this.orderType = orderType;
        this.rowFilter = rowFilter;
        this.columnFilter = columnFilter;
        this.valueFilter = valueFilter;
        tableType = bigtable.getType();
    }

    /**
     * Closestream.
     */
    public void closestream() {
        reset();
    }

    /** Retrieve the next record in a sequential scan
     *
     * @exception InvalidMapSizeException Invalid map size
     * @exception IOException I/O errors
     *
     * @param mid Record ID of the record
     * @return the Map of the retrieved record.
     */
    public Map getNext(MID mid)
            throws InvalidMapSizeException,
            IOException
    {
        Map recptrmap = null;

        if (nextUserStatus != true) {
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

        return recptrmap;
    }


    /** Position the scan cursor to the record with the given mid.
     *
     * @exception InvalidMapSizeException Invalid map size
     * @exception IOException I/O errors
     * @param mid Record ID of the given record
     * @return 	true if successful,
     *			false otherwise.
     */
    public boolean position(MID mid)
            throws InvalidMapSizeException,
            IOException
    {
        MID    nxtmid = new MID();
        boolean bst;

        bst = peekNext(nxtmid);

        if (nxtmid.equals(mid)==true)
            return true;

        // This is kind lame, but otherwise it will take all day.
        PageId pgid = new PageId();
        pgid.pid = mid.pageNo.pid;

        if (!datapageId.equals(pgid)) {

            // reset everything and start over from the beginning
            reset();

            bst =  firstDataPage();

            if (bst != true)
                return bst;

            while (!datapageId.equals(pgid)) {
                bst = nextDataPage();
                if (bst != true)
                    return bst;
            }
        }

        // Now we are on the correct page.

        try{
            usermid = datapage.firstMap();
        }
        catch (Exception e) {
            e.printStackTrace();
        }


        if (usermid == null)
        {
            bst = false;
            return bst;
        }

        bst = peekNext(nxtmid);

        while ((bst == true) && (nxtmid != mid))
            bst = mvNext(nxtmid);

        return bst;
    }


    /** Do all the constructor work
     *
     * @exception InvalidMapSizeException Invalid map size
     * @exception IOException I/O errors
     *
     * @param hf A HeapFile object
     */
    private void init(Heapfile hf)
            throws InvalidMapSizeException,
            IOException
    {
        _hf = hf;

        firstDataPage();
    }


    /** Reset everything and unpin all pages. */
    private void reset()
    {

        if (datapage != null) {

            try{
                unpinPage(datapageId, false);
            }
            catch (Exception e){
                // 	System.err.println("SCAN: Error in Scan" + e);
                e.printStackTrace();
            }
        }
        datapageId.pid = 0;
        datapage = null;

        if (dirpage != null) {

            try{
                unpinPage(dirpageId, false);
            }
            catch (Exception e){
                //     System.err.println("SCAN: Error in Scan: " + e);
                e.printStackTrace();
            }
        }
        dirpage = null;

        nextUserStatus = true;

    }


    /** Move to the first data page in the file.
     * @exception InvalidMapSizeException Invalid map size
     * @exception IOException I/O errors
     * @return true if successful
     *         false otherwise
     */
    private boolean firstDataPage()
            throws InvalidMapSizeException,
            IOException
    {
        DataPageInfo dpinfo;
        Map        recmap = null;
        Boolean      bst;

        /** copy data about first directory page */

        dirpageId.pid = _hf._firstDirPageId.pid;
        nextUserStatus = true;

        /** get first directory page and pin it */
        try {
            dirpage  = new HFPage();
            pinPage(dirpageId, (Page) dirpage, false);
        }

        catch (Exception e) {
            //    System.err.println("SCAN Error, try pinpage: " + e);
            e.printStackTrace();
        }

        /** now try to get a pointer to the first datapage */
        datapageMid = dirpage.firstMap();

        if (datapageMid != null) {
            /** there is a datapage record on the first directory page: */

            try {
                recmap = dirpage.getMap(datapageMid);
            }

            catch (Exception e) {
                //	System.err.println("SCAN: Chain Error in Scan: " + e);
                e.printStackTrace();
            }

            dpinfo = new DataPageInfo(recmap);
            datapageId.pid = dpinfo.pageId.pid;

        } else {

            /** the first directory page is the only one which can possibly remain
             * empty: therefore try to get the next directory page and
             * check it. The next one has to contain a datapage record, unless
             * the heapfile is empty:
             */
            PageId nextDirPageId = new PageId();

            nextDirPageId = dirpage.getNextPage();

            if (nextDirPageId.pid != INVALID_PAGE) {

                try {
                    unpinPage(dirpageId, false);
                    dirpage = null;
                }

                catch (Exception e) {
                    //	System.err.println("SCAN: Error in 1stdatapage 1 " + e);
                    e.printStackTrace();
                }

                try {

                    dirpage = new HFPage();
                    pinPage(nextDirPageId, (Page )dirpage, false);

                }

                catch (Exception e) {
                    //  System.err.println("SCAN: Error in 1stdatapage 2 " + e);
                    e.printStackTrace();
                }

                /** now try again to read a data record: */

                try {
                    datapageMid = dirpage.firstMap();
                }

                catch (Exception e) {
                    //  System.err.println("SCAN: Error in 1stdatapg 3 " + e);
                    e.printStackTrace();
                    datapageId.pid = INVALID_PAGE;
                }

                if(datapageMid != null) {

                    try {

                        recmap = dirpage.getMap(datapageMid);
                    }

                    catch (Exception e) {
                        //    System.err.println("SCAN: Error getRecord 4: " + e);
                        e.printStackTrace();
                    }

                    if (recmap == null)
                        return false;

                    dpinfo = new DataPageInfo(recmap);
                    datapageId.pid = dpinfo.pageId.pid;

                } else {
                    // heapfile empty
                    datapageId.pid = INVALID_PAGE;
                }
            }//end if01
            else {// heapfile empty
                datapageId.pid = INVALID_PAGE;
            }
        }

        datapage = null;

        try{
            nextDataPage();
        }

        catch (Exception e) {
            //  System.err.println("SCAN Error: 1st_next 0: " + e);
            e.printStackTrace();
        }

        return true;

        /** ASSERTIONS:
         * - first directory page pinned
         * - this->dirpageId has Id of first directory page
         * - this->dirpage valid
         * - if heapfile empty:
         *    - this->datapage == NULL, this->datapageId==INVALID_PAGE
         * - if heapfile nonempty:
         *    - this->datapage == NULL, this->datapageId, this->datapageMid valid
         *    - first datapage is not yet pinned
         */

    }


    /** Move to the next data page in the file and
     * retrieve the next data page.
     *
     * @return 		true if successful
     *			false if unsuccessful
     */
    private boolean nextDataPage()
            throws InvalidMapSizeException,
            IOException
    {
        DataPageInfo dpinfo;

        boolean nextDataPageStatus;
        PageId nextDirPageId = new PageId();
        Map recmap = null;

        // ASSERTIONS:
        // - this->dirpageId has Id of current directory page
        // - this->dirpage is valid and pinned
        // (1) if heapfile empty:
        //    - this->datapage==NULL; this->datapageId == INVALID_PAGE
        // (2) if overall first record in heapfile:
        //    - this->datapage==NULL, but this->datapageId valid
        //    - this->datapageMid valid
        //    - current data page unpinned !!!
        // (3) if somewhere in heapfile
        //    - this->datapageId, this->datapage, this->datapageMid valid
        //    - current data page pinned
        // (4)- if the scan had already been done,
        //        dirpage = NULL;  datapageId = INVALID_PAGE

        if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
            return false;

        if (datapage == null) {
            if (datapageId.pid == INVALID_PAGE) {
                // heapfile is empty to begin with

                try{
                    unpinPage(dirpageId, false);
                    dirpage = null;
                }
                catch (Exception e){
                    //  System.err.println("Scan: Chain Error: " + e);
                    e.printStackTrace();
                }

            } else {

                // pin first data page
                try {
                    datapage  = new HFPage();
                    pinPage(datapageId, (Page) datapage, false);
                }
                catch (Exception e){
                    e.printStackTrace();
                }

                try {
                    usermid = datapage.firstMap();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                return true;
            }
        }

        // ASSERTIONS:
        // - this->datapage, this->datapageId, this->datapageMid valid
        // - current datapage pinned

        // unpin the current datapage
        try{
            unpinPage(datapageId, false /* no dirty */);
            datapage = null;
        }
        catch (Exception e){

        }

        // read next datapagerecord from current directory page
        // dirpage is set to NULL at the end of scan. Hence

        if (dirpage == null) {
            return false;
        }

        datapageMid = dirpage.nextMap(datapageMid);

        if (datapageMid == null) {
            nextDataPageStatus = false;
            // we have read all datapage records on the current directory page

            // get next directory page
            nextDirPageId = dirpage.getNextPage();

            // unpin the current directory page
            try {
                unpinPage(dirpageId, false /* not dirty */);
                dirpage = null;

                datapageId.pid = INVALID_PAGE;
            }

            catch (Exception e) {

            }

            if (nextDirPageId.pid == INVALID_PAGE)
                return false;
            else {
                // ASSERTION:
                // - nextDirPageId has correct id of the page which is to get

                dirpageId = nextDirPageId;

                try {
                    dirpage  = new HFPage();
                    pinPage(dirpageId, (Page)dirpage, false);
                }

                catch (Exception e){

                }

                if (dirpage == null)
                    return false;

                try {
                    datapageMid = dirpage.firstMap();
                    nextDataPageStatus = true;
                }
                catch (Exception e){
                    nextDataPageStatus = false;
                    return false;
                }
            }
        }

        // ASSERTION:
        // - this->dirpageId, this->dirpage valid
        // - this->dirpage pinned
        // - the new datapage to be read is on dirpage
        // - this->datapageM has the Mid of the next datapage to be read
        // - this->datapage, this->datapageId invalid

        // data page is not yet loaded: read its record from the directory page
        try {
            recmap = dirpage.getMap(datapageMid);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        if (recmap == null)
            return false;

        dpinfo = new DataPageInfo(recmap);
        datapageId.pid = dpinfo.pageId.pid;

        try {
            datapage = new HFPage();
            pinPage(dpinfo.pageId, (Page) datapage, false);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }


        // - directory page is pinned
        // - datapage is pinned
        // - this->dirpageId, this->dirpage correct
        // - this->datapageId, this->datapage, this->datapageMid correct

        usermid = datapage.firstMap();

        if(usermid == null)
        {
            nextUserStatus = false;
            return false;
        }

        return true;
    }


    private boolean peekNext(MID mid) {

        mid.pageNo.pid = usermid.pageNo.pid;
        mid.slotNo = usermid.slotNo;
        return true;

    }


    /** Move to the next record in a sequential scan.
     * Also returns the MID of the (new) current record.
     */
    private boolean mvNext(MID mid)
            throws InvalidMapSizeException,
            IOException
    {
        MID nextmid;
        boolean status;

        if (datapage == null)
            return false;

        nextmid = datapage.nextMap(mid);

        if( nextmid != null ){
            usermid.pageNo.pid = nextmid.pageNo.pid;
            usermid.slotNo = nextmid.slotNo;
            return true;
        } else {

            status = nextDataPage();

            if (status==true){
                mid.pageNo.pid = usermid.pageNo.pid;
                mid.slotNo = usermid.slotNo;
            }

        }
        return true;
    }

    /**
     * short cut to access the pinPage function in bufmgr package.
     * @see bufmgr.pinPage
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Scan.java: pinPage() failed");
        }

    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     * @see bufmgr.unpinPage
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Scan.java: unpinPage() failed");
        }

    } // end of unpinPage
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
            case 1:
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
                        start = new StringKey(columnRange[0] + "$" + rowRange[0]);
                        end = new StringKey(columnRange[1] + "$" + rowRange[1] + this.lastChar);

                        //check row range and column fixed/*
                    } else if ((rowFilter.matches(rangeRegex)) && (!columnFilter.matches(rangeRegex))) {
                        String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        if (columnFilter.equals(starFilter)) {
                            scanAll = true;
                        } else {
                            start = new StringKey(columnFilter + "$" + rowRange[0]);
                            end = new StringKey(columnFilter + "$" + rowRange[1] + this.lastChar);
                        }
                        // check column range and row fixed/*
                    } else if ((!rowFilter.matches(rangeRegex)) && (columnFilter.matches(rangeRegex))) {
                        String[] columnRange = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
                        if (rowFilter.equals(starFilter)) {
                            start = new StringKey(columnRange[0]);
                            end = new StringKey(columnRange[1] + this.lastChar);
                        } else {

                            start = new StringKey(columnRange[0] + "$" + rowFilter);
                            end = new StringKey(columnRange[1] + "$" + rowFilter + this.lastChar);
                        }

                        //row and col are fixed val or *,fixed fixed,*
                    } else {
                        if (columnFilter.equals(starFilter)) {
                            scanAll = true;
                        } else if (rowFilter.equals(starFilter)) {
                            start = end = new StringKey(columnFilter);
                        } else {
                            start = new StringKey(columnFilter + "$" + rowFilter);
                            end = new StringKey(columnFilter + "$" + rowFilter + this.lastChar);
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
                        start = new StringKey(rowRange[0] + "$" + valueRange[0]);
                        end = new StringKey(rowRange[1] + "$" + valueRange[1] + this.lastChar);

                        //check row range and column fixed/*
                    } else if ((valueFilter.matches(rangeRegex)) && (!rowFilter.matches(rangeRegex))) {
                        String[] valueRange = valueFilter.replaceAll("[\\[ \\]]", "").split(",");
                        if (rowFilter.equals(starFilter)) {
                            scanAll = true;
                        } else {
                            start = new StringKey(rowFilter + "$" + valueRange[0]);
                            end = new StringKey(rowFilter + "$" + valueRange[1] + this.lastChar);
                        }
                        // check column range and row fixed/*
                    } else if ((!valueFilter.matches(rangeRegex)) && (rowFilter.matches(rangeRegex))) {
                        String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
                        if (valueFilter.equals("*")) {
                            start = new StringKey(rowRange[0]);
                            end = new StringKey(rowRange[1] + this.lastChar);
                        } else {

                            start = new StringKey(rowRange[0] + "$" + valueFilter);
                            end = new StringKey(rowRange[1] + "$" + valueFilter + this.lastChar);
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
                            start = new StringKey(rowFilter + "$" + valueFilter);
                            end = new StringKey(rowFilter + "$" + valueFilter + this.lastChar);
                        }
                    }
                }
                break;
        }

        if (!this.scanAll) {
            this.btreeScanner = bigtable.indexFiles[indexType].new_scan(start, end);
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

        Heapfile tempHeapFile = new Heapfile("tempSort");

        MID mid = new MID();
        if (this.scanAll) {
            //scanning whole bigt file.
            scan = bigtable.getHeapFile().openScan();

            //mapObj.setHeader();
            Map map = null;

//            if (rowFilter.equals(starFilter) && columnFilter.equals(starFilter) && valueFilter.equals(starFilter)) {
//                System.out.println("rowFilter = " + rowFilter);
//                tempHeapFile = this.bigtable.heapfile;
//            } else {

            int count = 0;
            map = this.scan.getNext(mid);
            while (map != null) {
                count++;
                short kaka = 0;
                if (genericMatcher(map, "row", rowFilter) && genericMatcher(map, "column", columnFilter) && genericMatcher(map, "value", valueFilter)) {
                    tempHeapFile.insertMap(map.getMapByteArray());
                }
                map = scan.getNext(mid);
            }

        } else {

            KeyDataEntry entry = btreeScanner.get_next();
            while (entry != null) {
                MID currMid = ((LeafData) entry.data).getData();
                if (currMid != null) {
                    MID tempMid = new MID(currMid.pageNo, currMid.slotNo);
                    Map map = bigtable.getHeapFile().getMap(tempMid);
                    if (genericMatcher(map, "row", rowFilter) && genericMatcher(map, "column", columnFilter) && genericMatcher(map, "value", valueFilter)) {
                        tempHeapFile.insertMap(map.getMapByteArray());
                    }

                }
                entry = btreeScanner.get_next();
            }
        }


        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projection[0] = new FldSpec(rel, 1);
        projection[1] = new FldSpec(rel, 2);
        projection[2] = new FldSpec(rel, 3);
        projection[3] = new FldSpec(rel, 4);

        FileScan fscan = null;
        AttrType[] attrTypes = new AttrType[]{new AttrType(0), new AttrType(0), new AttrType(1), new AttrType(0)};

        try {
            fscan = new FileScan("tempSort4", attrTypes, new short[]{(short) (32 * 1024), (short) (32 * 1024), (short) (32 * 1024)}, (short) 4, 4, projection, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        int sortField, num_pages = 10, sortFieldLength;
        switch (orderType) {
            case 1:
            case 3:
                sortField = 1;
                sortFieldLength = 32 * 1024;
                break;
            case 2:
            case 4:
                sortField = 2;
                sortFieldLength = 32 * 1024;
                break;
            case 5:
                sortField = 3;
                sortFieldLength = 32 * 1024;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + orderType);
        }
        try {
            this.sort = new MapSort(attrTypes, new short[]{(short) (32 * 1024), (short) (32 * 1024), (short) (32 * 1024)}, fscan, sortField, new MapOrder(MapOrder.Ascending), num_pages, sortFieldLength);
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

