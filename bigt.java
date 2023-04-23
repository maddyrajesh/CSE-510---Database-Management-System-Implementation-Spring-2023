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
import global.*;
import heap.*;

import java.io.*;
import java.util.*;

/**
 * The type Bigt.
 */
public class bigt {

    public final String name;
    public int counter = 0;
    private int type;
    private HashSet<String> rowSet;
    private HashSet<String> columnSet;
    private Heapfile heapFile;
    public BTreeFile btree1;
    public BTreeFile btree2;
    public BTreeFile[] indexFiles;
    private HashMap<ArrayList<String>, ArrayList<MID>> indexedMap;
    //BTreeFile[] indexFiles = new BTreeFile[2];

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
    public bigt(String name, int type) throws HFDiskMgrException, HFException, HFBufMgrException, IOException, ConstructPageException, AddFileEntryException, GetFileEntryException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException, SpaceNotAvailableException, InvalidSlotNumberException {
        this.name = name;
        this.type = type;
        this.heapFile = new Heapfile(name + ".heap");
        this.indexedMap = new HashMap<>();
        this.rowSet = new HashSet<>();
        this.columnSet = new HashSet<>();
        //SystemDefs.
        // metadata file
        Heapfile metaHeapFile = new Heapfile(name + "_meta.heap");
        Tuple metaTuple = new Tuple();
        metaTuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
        metaTuple.setIntFld(1, this.type);
        metaHeapFile.insertRecord(metaTuple.getTupleByteArray());

        switch(type)
        {
            default:
            case 1:
                break;
            // one btree to index row labels
            case 2:
                this.btree1 = new BTreeFile("rowIndex", AttrType.attrString, BigTable.BIGT_STR_SIZES[0], 0);
                break;
            // one btree to index column labels
            case 3:
                this.btree1 = new BTreeFile("columnIndex", AttrType.attrString, BigTable.BIGT_STR_SIZES[1], 0);
                break;
            /*
            one btree to index column label and row label (combined key)
             */
            case 4:
                this.btree1 = new BTreeFile("rowColumnIndex", AttrType.attrString, BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[1], 0);
                break;
            /*
            one btree to index row label and value (combined key)
             */
            case 5:
                this.btree1 = new BTreeFile("rowValueIndex", AttrType.attrString, BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[2], 0);
                break;
        }
    }

    public bigt(String name) {

        this.name = name;
        try {
            PageId heapFileId = SystemDefs.JavabaseDB.get_file_entry(name + "_meta.heap");
            if (heapFileId == null) {
                throw new Exception("BigT File with name: " + name + " doesn't exist");
            }
            /*PageId btreeFileId = SystemDefs.JavabaseDB.get_file_entry("columnIndex");
            if (btreeFileId == null) {
                throw new Exception("BigT File with name: columnIndex doesn't exist");
            }*/

            // Load the metadata from .meta heapfile
            Heapfile metaHeapFile = new Heapfile(name + "_meta.heap");
            Scan metaScan = metaHeapFile.openScan();
            Tuple metaTuple = metaScan.getNextTuple(new MID());
            metaTuple.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
            metaScan.closescan();
            this.type = metaTuple.getIntFld(1);

            // Set the Indexfile names from the type
            switch (type) {
                case 1:
                    break;
                // one btree to index row labels
                case 2:
                    this.btree1 = new BTreeFile("rowIndex");
                    break;
                // one btree to index column labels
                case 3:
                    this.btree1 = new BTreeFile("columnIndex");
                    break;
            /*
            one btree to index column label and row label (combined key) and
             */
                case 4:
                    this.btree1 = new BTreeFile("rowColumnIndex");
                    break;
            /*
            one btree to index row label and value (combined key)
             */
                case 5:
                    this.btree1 = new BTreeFile("rowValueIndex");
                    break;
            }
                // Open the Heap file which is used for storing the maps
                this.heapFile = new Heapfile(name + ".heap");

                // Load the mapVersion HashMap from the disk
                try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(this.name + ".hashmap"))) {
                    this.type = objectInputStream.readByte();
                    this.indexedMap = (HashMap<ArrayList<String>, ArrayList<MID>>) objectInputStream.readObject();
                    this.rowSet = (HashSet<String>) objectInputStream.readObject();
                    this.columnSet = (HashSet<String>) objectInputStream.readObject();
                    Integer tmpCounter = (Integer) objectInputStream.readObject();
                    this.counter = tmpCounter;
                } catch (IOException e) {
                    throw new IOException("File not writable: " + e.toString());
                }


            } catch (ConstructPageException e) {
            throw new RuntimeException(e);
        } catch (GetFileEntryException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InvalidMapSizeException e) {
            throw new RuntimeException(e);
        } catch (FieldNumberOutOfBoundException e) {
            throw new RuntimeException(e);
        } catch (AddFileEntryException e) {
            throw new RuntimeException(e);
        } catch (HFDiskMgrException e) {
            throw new RuntimeException(e);
        } catch (HFException e) {
            throw new RuntimeException(e);
        } catch (InvalidPageNumberException e) {
            throw new RuntimeException(e);
        } catch (HFBufMgrException e) {
            throw new RuntimeException(e);
        } catch (InvalidTupleSizeException e) {
            throw new RuntimeException(e);
        } catch (FileIOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (DiskMgrException e) {
            throw new RuntimeException(e);
        } catch (InvalidTypeException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

        public void close() throws PageUnpinnedException, PagePinnedException, PageNotFoundException, HashOperationException, BufMgrException, IOException, HashEntryNotFoundException, InvalidFrameNumberException, ReplacerException {
        if(this.btree1 != null)
            this.btree1.close();

        if(this.btree2 != null)
            this.btree2.close();

        new File(this.name + ".hashmap").delete();
        File file = new File(this.name + ".hashmap");
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(this.name + ".hashmap"));
        objectOutputStream.writeByte(type);
        objectOutputStream.writeObject(indexedMap);
        objectOutputStream.writeObject(rowSet);
        objectOutputStream.writeObject(columnSet);
        Integer tmpCounter = counter;
        objectOutputStream.writeObject(tmpCounter);
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
    public void deleteBigt() throws Exception {
        heapFile.deleteFile();
        btree1.destroyFile();
        btree2.destroyFile();
    }


    public void createIndex(Integer type) throws Exception {
        switch (type) {
            default:
            case 1:
                break;
            // one btree to index row labels
            case 2:
                this.btree1 = new BTreeFile("rowIndex", AttrType.attrString, BigTable.BIGT_STR_SIZES[0], 0);
                break;
            // one btree to index column labels
            case 3:
                this.btree1 = new BTreeFile("columnIndex", AttrType.attrString, BigTable.BIGT_STR_SIZES[1], 0);
                break;
            /*
            one btree to index column label and row label (combined key)
             */
            case 4:
                this.btree1 = new BTreeFile("rowColumnIndex", AttrType.attrString, BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[1], 0);
                break;
            /*
            one btree to index row label and value (combined key)
             */
            case 5:
                this.btree1 = new BTreeFile("rowValueIndex", AttrType.attrString, BigTable.BIGT_STR_SIZES[0] + BigTable.BIGT_STR_SIZES[2], 0);
                break;
        }


        Scan scan = heapFile.openScan();
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
     * Gets map cnt.
     *
     * @return the map cnt
     * @throws InvalidMapSizeException    the invalid map size exception
     * @throws HFDiskMgrException         the hf disk mgr exception
     * @throws InvalidSlotNumberException the invalid slot number exception
     * @throws HFBufMgrException          the hf buf mgr exception
     * @throws IOException                the io exception
     */
    public int getMapCnt() throws InvalidMapSizeException, HFDiskMgrException, InvalidSlotNumberException, HFBufMgrException, IOException, InvalidTupleSizeException {
        return heapFile.getRecCnt();
    }

    /**
     * Gets row cnt.
     *
     * @return the row cnt
     */
    public int getRowCnt() {
        return rowSet.size();
    }

    /**
     * Gets column cnt.
     *
     * @return the column cnt
     */
    public int getColumnCnt() {
        return columnSet.size();
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
                    timestampMap.put(heapFile.getMap(mid).getTimeStamp(), mid);
                }
            }

            // if there are three timestamps already, remove oldest
            if (timestampMap.size() == 3)
            {
                int oldestTimestamp = Collections.min(timestampMap.keySet());
                if (map.getTimeStamp() > oldestTimestamp)
                {
                    // delete record
                    heapFile.deleteMap(timestampMap.get(oldestTimestamp));
                    MID newMID = heapFile.insertMap(mapPtr);

                    // update indexedMap
                    indexedMap.get(rowColumnKey).remove(timestampMap.get(oldestTimestamp));
                    indexedMap.get(rowColumnKey).add(newMID);

                    // update indexes
                    insertIndex(newMID, type);
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
                MID newMID = heapFile.insertMap(mapPtr);
                // MID with same row column key already exists
                if (indexedMap.containsKey(rowColumnKey))
                {
                    indexedMap.get(rowColumnKey).add(newMID);
                }
                // MID with same row column key doesn't exist, needs to be put
                else
                {
                    indexedMap.put(rowColumnKey, new ArrayList<MID>(Arrays.asList(newMID)));
                }
                insertIndex(newMID, type);

                // update row and column sets
                rowSet.add(map.getRowLabel());
                columnSet.add(map.getColumnLabel());

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
    public void insertIndex(MID mid, int type) throws Exception {
        Map map = heapFile.getMap(mid);
        switch(type)
        {
            case 2:
                btree1.insert(new StringKey(map.getRowLabel()), mid);
                break;
            case 3:
                btree1.insert(new StringKey(map.getColumnLabel()), mid);
                break;
            case 4:
                btree1.insert(new StringKey(map.getRowLabel() + map.getColumnLabel()), mid);
                break;
            case 5:
                btree1.insert(new StringKey(map.getRowLabel() + map.getValue()), mid);
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
        Map map = heapFile.getMap(mid);
        switch(type)
        {
            case 2:
                btree1.Delete(new StringKey(map.getRowLabel()), mid);
                break;
            case 3:
                btree1.Delete(new StringKey(map.getColumnLabel()), mid);
                break;
            case 4:
                btree1.Delete(new StringKey(map.getRowLabel() + map.getColumnLabel()), mid);
                break;
            case 5:
                btree1.Delete(new StringKey(map.getRowLabel() + map.getValue()), mid);
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
    public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
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

    public Heapfile getHeapFile() {
        return heapFile;
    }
}
