package BigT;

import global.*;
import java.io.*;

/**
 * The type Map.
 */
public class Map implements GlobalConst {
    // 3 strings with max size 64kb and an int with size 4b
    public static final int max_size = 64 * 1024 * 3 + 4;
    public static final short fldCnt = 4;
    private byte [] data;
    private int map_offset;
    private short [] fldOffset;

    /**
     * Instantiates a new Map.
     */
    public Map() {
        data = new byte[max_size];
        map_offset = 0;
    }

    /**
     * Instantiates a new Map.
     *
     * @param amap   the amap
     * @param offset the offset
     */
    public Map(byte[] amap, int offset) {
        data = amap;
        map_offset = offset;
    }

    /**
     * Instantiates a new Map.
     *
     * @param fromMap the from map
     */
    public Map(Map fromMap) {
        data = fromMap.getMapByteArray();
        map_offset = 0;
        fldOffset = fromMap.copyFldOffset();
    }

    private short[] copyFldOffset() {
        short[] newFldOffset = new short[fldCnt + 1];
        System.arraycopy(fldOffset, 0, newFldOffset, 0, fldOffset.length);
        return newFldOffset;
    }

    /**
     * Sets the fixed map header
     *
     * @param strSizes the from map
     * @exception   IOException I/O errors
     */
    public void setHdr(short[] strSizes) throws IOException {
        fldOffset = new short[5];
        int pos = map_offset + 2;

        fldOffset[0] = (short) ((fldCnt + 2) * 2 + map_offset);

        Convert.setShortValue(fldOffset[0], pos, data);

        for (int i = 1; i <= fldCnt; i++) {
            pos += 2;
            // timestamp increments by 4, otherwise increment by string size
            if (i == 4) {
                fldOffset[i] = (short) (fldOffset[i - 1] + 4);
            } else {
                fldOffset[i] = (short) (fldOffset[i - 1] + (short) strSizes[i - 1] + 2); // strlen in bytes = strlen + 2
            }
            Convert.setShortValue(fldOffset[i], pos, data);
        }
    }

    /**
     * Gets row label.
     *
     * @return the row label
     * @exception   IOException I/O errors
     */
    public String getRowLabel() throws IOException {
        return Convert.getStrValue(fldOffset[0], data, fldOffset[1] - fldOffset[0]);
    }

    /**
     * Gets column label.
     *
     * @return the column label
     * @exception   IOException I/O errors
     */
    public String getColumnLabel() throws IOException {
        return Convert.getStrValue(fldOffset[1], data, fldOffset[2] - fldOffset[1]);
    }

    /**
     * Gets time stamp.
     *
     * @return the time stamp
     * @exception   IOException I/O errors
     */
    public int getTimeStamp() throws IOException {
        return Convert.getIntValue(fldOffset[2], data);
    }

    /**
     * Gets value.
     *
     * @return the value
     * @exception   IOException I/O errors
     */
    public String getValue() throws IOException {
        return Convert.getStrValue(fldOffset[3], data, fldOffset[3] - fldOffset[2]);
    }

    /**
     * Sets row label.
     *
     * @param val the string value
     * @return the map with updated row label
     * @exception   IOException I/O errors
     */
    public Map setRowLabel(String val) throws IOException {
        Convert.setStrValue(val, fldOffset[0], data);
        return this;
    }

    /**
     * Sets column label.
     *
     * @param val the string value
     * @return the map with updated column label
     * @exception   IOException I/O errors
     */
    public Map setColumnLabel(String val) throws IOException {
        Convert.setStrValue(val, fldOffset[1], data);
        return this;
    }

    /**
     * Sets time stamp.
     *
     * @param val the string value
     * @return the map with updated time stamp
     * @exception   IOException I/O errors
     */
    public Map setTimeStamp(int val) throws IOException {
        Convert.setIntValue(val, fldOffset[2], data);
        return this;
    }

    /**
     * Sets value.
     *
     * @param val the val
     * @return the map with updated value
     * @exception   IOException I/O errors
     */
    public Map setValue(String val) throws IOException {
        Convert.setStrValue(val, fldOffset[1], data);
        return this;
    }

    /**
     * Returns the current map byte array
     *
     * @return a byte array containing the map
     */
    public byte[] getMapByteArray() {
        byte [] mapcopy = new byte [max_size];
        System.arraycopy(data, map_offset, mapcopy, 0, size());
        return mapcopy;
    }

    /**
     * Print out the map
     *
     * @exception   IOException I/O errors
     */
    public void print() throws IOException {
        System.out.print("[row: " + getRowLabel() + ", column: " + getColumnLabel() + ", time: " + getTimeStamp() + ", value: " + getValue() + "]");
    }

    /**
     * Get the length of a map, call this method if you
     *  called setHdr() before
     * @return size of this map in bytes
     */
    public short size() {
        return ((short) (fldOffset[fldCnt] - map_offset));
    }

    /**
     * Copy a map to the current map position
     *
     * @param fromMap the map to be copied
     */
    public void mapCopy(Map fromMap) {
        byte [] temparray = fromMap.getMapByteArray();
        System.arraycopy(temparray, 0, data, map_offset, fromMap.size());
    }

    /**
     * This is used when you don't want to use the constructor
     *
     * @param amap   a byte array which contains the map
     * @param offset the offset of amap
     */
    public void mapInit(byte[] amap, int offset) {
        data = amap;
        map_offset = offset;
    }

    /**
     * Set a map with the given map and offset
     *
     * @param frommap a byte array containing the map
     * @param offset  the offset of the map
     */
    public void mapSet(byte[] frommap, int offset) {
        System.arraycopy(frommap, offset, data, 0, frommap.length);
    }
}

