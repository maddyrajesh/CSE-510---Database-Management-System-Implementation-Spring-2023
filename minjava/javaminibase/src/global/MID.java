package global;

import java.io.*;

/** class MID modeled off of RID
 */

public class MID {

    public int slotNo;
    public PageId pageNo = new PageId();

    public MID () {}

    public MID (PageId pageno, int slotno) {
        pageNo = pageno;
        slotNo = slotno;
    }

    public void copyMid(MID mid) {
        pageNo = mid.pageNo;
        slotNo = mid.slotNo;
    }

    public void writeToByteArray(byte [] ary, int offset) throws java.io.IOException {
        Convert.setIntValue ( slotNo, offset, ary);
        Convert.setIntValue ( pageNo.pid, offset+4, ary);
    }

    public boolean equals(MID mid) {
        if ((this.pageNo.pid==mid.pageNo.pid) &&(this.slotNo==mid.slotNo))
            return true;
        else
            return false;
    }
}
