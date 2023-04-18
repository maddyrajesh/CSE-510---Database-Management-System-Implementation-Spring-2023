package BigT;

/*import cmdline.MiniTable;*/
import driver.BigTable;
/*import global.TupleOrder;*/
import global.MapOrder;
import heap.Heapfile;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.MapSort;
import iterator.RelSpec;

public class rowSort {

    private String column;
    private Stream mapStream;
    private bigt bigTable;
    private Heapfile heapfile;
    private MapSort sortObj;
    private int numBuffers;

    public rowSort(String bigTable, String column, int numBuffers) throws Exception {
        this.column = column;
        this.numBuffers = numBuffers;
        this.bigTable = new bigt(bigTable, 1); /*type 1 BigTable*/
        this.heapfile = new Heapfile("temp_sort_file");
        insertTempHeapFile();
        createMapStream();

    }


    private void insertTempHeapFile() throws Exception {

        BigTable.orderType = 1;
        Stream tempStream = this.bigTable.openStream(1, "*", "*", "*");
        Map map = tempStream.getNext();
        String value = "";
        String row = map.getRowLabel(); //previous row

        while(map != null)
        {
            if(!map.getRowLabel().equals(row)){
                if (value.equals("")) {
                    value = "0";
                }
                Map tempMap = new Map();
                tempMap.setHeader(BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES);
                tempMap.setRowLabel(row);
                tempMap.setColumnLabel("temp_column");
                tempMap.setValue(value);
                tempMap.setTimeStamp(1);
                this.heapfile.insertMap(tempMap.getMapByteArray());
                row = map.getRowLabel();
                value = "";
            }
            if(map.getColumnLabel().equals(this.column)){
                value = map.getValue();
            }
            map = tempStream.getNext();

        }

        tempStream.closestream();

        Map tempMap = new Map();
        tempMap.setHeader(BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES);
        tempMap.setRowLabel(row);
        tempMap.setColumnLabel("temp_column");
        if (value == "") {
            value = "0";
        }
        tempMap.setValue(value);
        tempMap.setTimeStamp(1);
        this.heapfile.insertMap(tempMap.getMapByteArray());
    }

    private void createMapStream() throws Exception {
        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projection[0] = new FldSpec(rel, 1);
        projection[1] = new FldSpec(rel, 2);
        projection[2] = new FldSpec(rel, 3);
        projection[3] = new FldSpec(rel, 4);

        FileScan fscan = null;
        try {
            fscan = new FileScan("temp_sort_file", BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES, (short) 4, 4, projection, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            BigTable.orderType = 1;
            this.sortObj = new MapSort(BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES, fscan, 4, new MapOrder(MapOrder.Ascending), 20, BigTable.BIGT_STR_SIZES[1], BigTable.orderType);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Map map = sortObj.get_next();
        this.mapStream = this.bigTable.openStream(1, map.getRowLabel(), "*", "*");


    }

    public Map getNext() throws Exception {
        Map map = this.mapStream.getNext();
        if(map == null){
            this.mapStream.closestream();
            BigTable.orderType = 1;
            Map nextVal = this.sortObj.get_next();
            if (nextVal == null) {
                return null;
            }
            this.mapStream = this.bigTable.openStream(1, nextVal.getRowLabel(), "*", "*");
            map = this.mapStream.getNext();
            if(map == null)
                this.mapStream.closestream();
        }

        return map;
    }

    public void closeStream() throws Exception{
        this.sortObj.close();
        heapfile.deleteFile();
    }



}