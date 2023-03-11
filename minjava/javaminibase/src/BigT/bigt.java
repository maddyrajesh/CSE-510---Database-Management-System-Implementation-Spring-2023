package BigT;

import global.*;

/**
 * The type Bigt.
 */
public class bigt {

    private final String name;
    private final int type;
    private int mapCount = 0;
    private int rowCount = 0;
    private int columnCount = 0;
    /**
     * Instantiates a new Bigt.
     *
     * @param name the name
     * @param type the type
     */
    public bigt(String name, int type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Delete bigt.
     */
    public void deleteBigt() {

    }

    /**
     * Gets map cnt.
     *
     * @return the map cnt
     */
    public int getMapCnt() {
        return mapCount;
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
     */
    public MID insertMap (byte[] mapPtr) {
        return null;
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
        return null;
    }
}
