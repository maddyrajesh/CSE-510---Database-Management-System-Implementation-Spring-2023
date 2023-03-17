package BigT;

import btree.*;
import global.*;
import heap.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * The type Bigt.
 */
public class bigt {

    private final String name;
    private final int type;
    private int mapCount = 0;
    private int rowCount = 0;
    private int columnCount = 0;
    private Heapfile heapFile;
    private BTreeFile btree1;
    private BTreeFile btree2;
    private HashMap<ArrayList<String>, ArrayList<MID>> indexedMap;

    /**
     * Instantiates a new Bigt.
     *
     * @param name the name
     * @param type the type
     * @throws HFDiskMgrException     the hf disk mgr exception
     * @throws HFException            the hf exception
     * @throws HFBufMgrException      the hf buf mgr exception
     * @throws IOException            the io exception
     * @throws ConstructPageException the construct page exception
     * @throws AddFileEntryException  the add file entry exception
     * @throws GetFileEntryException  the get file entry exception
     */
    public bigt(String name, int type) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, ConstructPageException, AddFileEntryException, GetFileEntryException {
        this.name = name;
        this.type = type;
        this.heapFile = new Heapfile(name);

        switch(type)
        {
            // one btree to index row labels
            case 1:
                this.btree1 = new BTreeFile("rowIndex", AttrType.attrString, Map.max_string_size, 0);
                break;
            // one btree to index column labels
            case 2:
                this.btree1 = new BTreeFile("columnIndex", AttrType.attrString, Map.max_string_size, 0);
                break;
            /*
            one btree to index column label and row label (combined key) and
            one btree to index timestamps
             */
            case 3:
                this.btree1 = new BTreeFile("rowColumnIndex", AttrType.attrString, Map.max_string_size * 2, 0);
                this.btree2 = new BTreeFile("timestampIndex", AttrType.attrString, Map.max_int_size, 0);
                break;
            /*
            one btree to index row label and value (combined key) and
            one btree to index timestamps
             */
            case 4:
                this.btree1 = new BTreeFile("rowValueIndex", AttrType.attrString, Map.max_string_size * 2, 0);
                this.btree2 = new BTreeFile("timestampIndex", AttrType.attrString, Map.max_int_size, 0);
                break;
            /*
            one btree to index column label and value (combined key) and
            one btree to index timestamps
             */
            case 5:
                this.btree1 = new BTreeFile("columnValueIndex", AttrType.attrString, Map.max_string_size * 2, 0);
                this.btree2 = new BTreeFile("timestampIndex", AttrType.attrString, Map.max_int_size, 0);
                break;
        }
    }

    /**
     * Delete bigt.
     *
     * @throws InvalidMapSizeException     the invalid map size exception
     * @throws HFDiskMgrException          the hf disk mgr exception
     * @throws InvalidSlotNumberException  the invalid slot number exception
     * @throws HFBufMgrException           the hf buf mgr exception
     * @throws FileAlreadyDeletedException the file already deleted exception
     * @throws IOException                 the io exception
     * @throws IteratorException           the iterator exception
     * @throws ConstructPageException      the construct page exception
     * @throws PinPageException            the pin page exception
     * @throws UnpinPageException          the unpin page exception
     * @throws FreePageException           the free page exception
     * @throws DeleteFileEntryException    the delete file entry exception
     */
    public void deleteBigt() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, FileAlreadyDeletedException, IOException, IteratorException, ConstructPageException, PinPageException, UnpinPageException, FreePageException, DeleteFileEntryException {
        heapFile.deleteFile();
        btree1.destroyFile();
        btree2.destroyFile();
        mapCount = 0;
        rowCount = 0;
        columnCount = 0;
    }

    /**
     * Gets map cnt.
     *
     * @return the map cnt
     * @throws InvalidMapSizeException    the invalid map size exception
     * @throws HFDiskMgrException         the hf disk mgr exception
     * @throws InvalidSlotNumberException the invalid slot number exception
     * @throws HFBufMgrException          the hf buf mgr exception
     * @throws IOException                the io exception
     */
    public int getMapCnt() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException {
        return heapFile.getRecCnt();
    }

    /**
     * Gets row cnt.
     *
     * @return the row cnt
     */
    public int getRowCnt() {
        return rowCount;
    }

    /**
     * Gets column cnt.
     *
     * @return the column cnt
     */
    public int getColumnCnt() {
        return columnCount;
    }

    /**
     * Insert map mid.
     *
     * @param mapPtr the map ptr
     * @return the mid
     * @throws Exception the exception
     */
    public MID insertMap (byte[] mapPtr) throws Exception {
        try
        {
            Map map = new Map(mapPtr, 0);
            HashMap<Integer, MID> timestampMap = new HashMap<Integer, MID>();
            ArrayList<String> rowColumnKey = new ArrayList<String>();
            rowColumnKey.add(map.getRowLabel());
            rowColumnKey.add(map.getColumnLabel());

            // get all maps with same row and column label as map to be inserted
            if (indexedMap.containsKey(rowColumnKey))
            {
                for (MID mid : indexedMap.get(rowColumnKey))
                {
                    timestampMap.put(heapFile.getRecord(mid).getTimeStamp(), mid);
                }
            }

            // if there are three timestamps already, remove oldest
            if (timestampMap.size() == 3)
            {
                int oldestTimestamp = Collections.min(timestampMap.keySet());
                if (map.getTimeStamp() > oldestTimestamp)
                {
                    // delete record
                    heapFile.deleteRecord(timestampMap.get(oldestTimestamp));
                    MID newMID = heapFile.insertRecord(mapPtr);

                    // update indexedMap
                    indexedMap.get(rowColumnKey).remove(timestampMap.get(oldestTimestamp));
                    indexedMap.get(rowColumnKey).add(newMID);

                    // update indexes
                    insertIndex(newMID);
                    deleteIndex(timestampMap.get(oldestTimestamp));

                    return newMID;
                }
                // the new map contains the oldest timestamp, so we don't change anything
                else
                {
                    return null;
                }
            }
            // timestamp limit has not been reached so just insert
            else
            {
                MID newMID = heapFile.insertRecord(mapPtr);
                indexedMap.get(rowColumnKey).add(newMID);
                insertIndex(newMID);
                return newMID;
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * Insert index.
     *
     * @param mid the mid
     * @throws Exception the exception
     */
    public void insertIndex(MID mid) throws Exception {
        Map map = heapFile.getRecord(mid);
        switch(type)
        {
            case 1:
                btree1.insert(new StringKey(map.getRowLabel()), mid);
                break;
            case 2:
                btree1.insert(new StringKey(map.getColumnLabel()), mid);
                break;
            case 3:
                btree1.insert(new StringKey(map.getRowLabel() + map.getColumnLabel()), mid);
                btree1.insert(new IntegerKey(map.getTimeStamp()), mid);
                break;
            case 4:
                btree1.insert(new StringKey(map.getRowLabel() + map.getValue()), mid);
                btree1.insert(new IntegerKey(map.getTimeStamp()), mid);
                break;
            case 5:
                btree1.insert(new StringKey(map.getColumnLabel() + map.getValue()), mid);
                btree1.insert(new IntegerKey(map.getTimeStamp()), mid);
                break;
        }
    }

    /**
     * Delete index.
     *
     * @param mid the mid
     * @throws Exception the exception
     */
    public void deleteIndex(MID mid) throws Exception {
        Map map = heapFile.getRecord(mid);
        switch(type)
        {
            case 1:
                btree1.Delete(new StringKey(map.getRowLabel()), mid);
                break;
            case 2:
                btree1.Delete(new StringKey(map.getColumnLabel()), mid);
                break;
            case 3:
                btree1.Delete(new StringKey(map.getRowLabel() + map.getColumnLabel()), mid);
                btree1.Delete(new IntegerKey(map.getTimeStamp()), mid);
                break;
            case 4:
                btree1.Delete(new StringKey(map.getRowLabel() + map.getValue()), mid);
                btree1.Delete(new IntegerKey(map.getTimeStamp()), mid);
                break;
            case 5:
                btree1.Delete(new StringKey(map.getColumnLabel() + map.getValue()), mid);
                btree1.Delete(new IntegerKey(map.getTimeStamp()), mid);
                break;
        }
    }

    /**
     * Open stream stream.
     *
     * @param orderType    the order type
     * @param rowFilter    the row filter
     * @param columnFilter the column filter
     * @param valueFilter  the value filter
     * @return the stream
     */
    public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) {
        // return stream as-is because class variable orderType will implicitly sort the Stream
        return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
    }


    /**
     * Gets type.
     *
     * @return the type
     */
    public int getType() {
        return type;
    }
}
