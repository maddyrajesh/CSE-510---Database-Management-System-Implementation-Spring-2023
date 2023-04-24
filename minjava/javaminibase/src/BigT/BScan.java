package BigT;





/**
 * JAVA
 * BScan.java-  class BScan
 */
/**
 * BScan.java-  class BScan
 *
 */

import java.io.*;

import global.*;
import heap.*;

/**
 * A NScan object is created ONLY through the function openScan
 * of a HeapFile. It supports the getNext interface which will
 * simply retrieve the next record in the heapfile.
 *
 * An object of type scan will always have pinned one directory page
 * of the heapfile.
 */
public class BScan extends Scan implements GlobalConst {

    /**
     * The constructor pins the first directory page in the file
     * and initializes its private data members from the private
     * data member from hf
     *
     * @param hf A HeapFile object
     * @throws InvalidTupleSizeException Invalid tuple size
     * @throws IOException               I/O errors
     * @throws InvalidMapSizeException 
     */
    public BScan(Heapfile hf) throws heap.InvalidMapSizeException, IOException, InvalidTupleSizeException{
        super(hf);
    }
}
