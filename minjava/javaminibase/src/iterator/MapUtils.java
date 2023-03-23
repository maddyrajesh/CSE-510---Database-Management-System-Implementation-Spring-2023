package iterator;

import BigT.Map;
import driver.BigTable;
import global.AttrType;

import java.io.IOException;

/**
 *some useful method when processing Tuple
 */
public class MapUtils
{
    public static int CompareMapWithMap(Map m1, Map m2, int map_fld_no)
            throws IOException,
            MapUtilsException,
            UnknowAttrType
    {
        int   m1_i,  m2_i;
        String m1_s, m2_s;

        switch (map_fld_no)
        {
            case 0:
                m1_s = m1.getRowLabel();
                m2_s = m2.getRowLabel();
                return Integer.compare(m1_s.compareTo(m2_s), 0);


            case 1:
                if (m1.getColumnLabel() == null) {
                    System.out.println("map1 column label NULL");
                    System.out.println("map1 = " + m1);
                }
                m1_s= m1.getColumnLabel();
                m2_s = m2.getColumnLabel();
                return Integer.compare(m1_s.compareTo(m2_s), 0);

            case 2:
                m1_i= m1.getTimeStamp();
                m2_i = m2.getTimeStamp();
                return Integer.compare(m1_i, m2_i);

            case 3:
                m1_s= m1.getValue();
                m2_s = m2.getValue();
                return Integer.compare(m1_s.compareTo(m2_s), 0);
            default:
                throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

        }
    }



    /**
     * This function  compares  tuple1 with another tuple2 whose
     * field number is same as the tuple1
     *

     *@param    m1        one map
     *@param    value     one string.
     *@param    m1_fld_no the field numbers in the maps to be compared.
     *@return   0        if the two are equal,
     *          1        if the map is greater,
     *         -1        if the map is smaller,
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */
    public static int CompareMapWithValue(Map m1, int m1_fld_no,
                                            Map  value)
            throws IOException,
            UnknowAttrType,
            MapUtilsException
    {
        return CompareMapWithMap(m1, value, m1_fld_no);
    }

    /**
     *This function Compares two Tuple inn all fields
     * @param m1 the first map
     * @param m2 the secocnd map
     * @return  0        if the two are not equal,
     *          1        if the two are equal,
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */

    public static boolean Equal(Map m1, Map m2)
            throws IOException,UnknowAttrType,MapUtilsException
    {
        int i;

        for (i = 0; i <= 3; i++)
            if (CompareMapWithMap(m1, m2, i) != 0)
                return false;
        return true;
    }

    /**
     *get the string specified by the field number
     *@param tuple the tuple
     *@param fidno the field number
     *@return the content of the field number
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */
    /*
    public static String Value(Tuple  tuple, int fldno)
            throws IOException,
            MapUtilsException
    {
        String temp;
        try{
            temp = tuple.getStrFld(fldno);
        }catch (FieldNumberOutOfBoundException e){
            throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
        }
        return temp;
    }
    */

    /**
     *set up a tuple in specified field from a tuple
     *@param m1 the map to be set
     *@param m2 the given tuple
     *@param map_fld_no the field number
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */
    public static void SetValue(Map m1, Map m2, int map_fld_no)
            throws IOException,
            UnknowAttrType,
            MapUtilsException
    {
        int m1_i, m2_i;
        String m1_s, m2_s;
        switch (map_fld_no)
        {
            case 1:
                m1.setRowLabel(m2.getRowLabel());
                break;
            case 2:
                m1.setColumnLabel(m2.getColumnLabel());
            case 3:
                m1.setTimeStamp(m2.getTimeStamp());
            case 4:
                m1.setValue(m2.getValue());
        }

        return;
    }


    /**
     *set up the Jmap's attrtype, string size,field number for using join
     *@param Jmap  reference to an actual map  - no memory has been malloced
     *@param res_attrs  attributes type of result map
     *@param in1  array of the attributes of the map (ok)
     *@param len_in1  length of attributes of in1
     *@param in2  array of the attributes of the map (ok)
     *@param len_in2  length of attributes of in2
     *@param t1_str_sizes shows the size of the string fields in S
     *@param t2_str_sizes shows the size of the string fields in R
     *@param proj_list shows what input fields go where in the output map
     *@param nOutFlds number of outer relation fileds
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */
    public static short[] setup_op_map(Map Jmap, AttrType[] res_attrs,
                                         AttrType in1[], int len_in1, AttrType in2[],
                                         int len_in2, short t1_str_sizes[],
                                         short t2_str_sizes[],
                                         FldSpec proj_list[], int nOutFlds)
            throws IOException,
            MapUtilsException
    {
        short [] sizesT1 = new short [len_in1];
        short [] sizesT2 = new short [len_in2];
        int i, count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        for (count = 0, i = 0; i < len_in2; i++)
            if (in2[i].attrType == AttrType.attrString)
                sizesT2[i] = t2_str_sizes[count++];

        int n_strs = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
            else if (proj_list[i].relation.key == RelSpec.innerRel)
                res_attrs[i] = new AttrType(in2[proj_list[i].offset-1].attrType);
        }

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
            else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short [n_strs];
        count         = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
            else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT2[proj_list[i].offset-1];
        }
        try {
            //Jmap.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
            Jmap.setHeader(BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES);
        }catch (Exception e){
            throw new MapUtilsException(e,"setHdr() failed");
        }
        return res_str_sizes;
    }


    /**
     *set up the Jmap's attrtype, string size,field number for using project
     *@param Jmap  reference to an actual map  - no memory has been malloced
     *@param res_attrs  attributes type of result map
     *@param in1  array of the attributes of the map (ok)
     *@param len_in1  length of attributes of in1
     *@param t1_str_sizes shows the size of the string fields in S
     *@param proj_list shows what input fields go where in the output tuple
     *@param nOutFlds number of outer relation fileds
     *@exception IOException some I/O fault
     *@exception TupleUtilsException exception from this class
     *@exception InvalidRelation invalid relation
     */

    public static short[] setup_op_map(Map Jmap, AttrType[] res_attrs,
                                       AttrType in1[], int len_in1,
                                       short t1_str_sizes[],
                                       FldSpec proj_list[], int nOutFlds)
            throws IOException,
            MapUtilsException, InvalidRelation {
        short [] sizesT1 = new short [len_in1];
        int i, count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        int n_strs = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
            else throw new InvalidRelation("Invalid relation: inner relation");
        }

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short [n_strs];
        count         = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
        }
        try {
            Jmap.setHeader(BigTable.BIGT_ATTR_TYPES, BigTable.BIGT_STR_SIZES);
        }catch (Exception e){
            throw new MapUtilsException(e,"setHdr() failed");
        }
        return res_str_sizes;
    }

    public static int CompareMapsOnOrderType(Map mapObj1, Map mapObj2) throws IOException {
        int mapRowCompare = mapObj1.getRowLabel().compareTo(mapObj2.getRowLabel());
        int mapColumnCompare = mapObj1.getColumnLabel().compareTo(mapObj2.getColumnLabel());
        int mapValueCompare = mapObj1.getValue().compareTo(mapObj2.getValue());
        boolean mapTsCompare = (mapObj1.getTimeStamp() >= mapObj2.getTimeStamp());

        if (BigTable.orderType == 2) {
            if (mapColumnCompare > 0) return 1;
            else if (mapColumnCompare < 0) return -1;
            else if (mapRowCompare > 0) return 1;
            else if (mapRowCompare < 0) return -1;
            else if (mapTsCompare) return 1;
            else return -1;
        } else if (BigTable.orderType == 3) {
            if (mapRowCompare > 0) return 1;
            else if (mapRowCompare < 0) return -1;
            else {
                if (mapTsCompare) return 1;
                else return -1;
            }
        } else if (BigTable.orderType == 4) {
            if (mapColumnCompare > 0) return 1;
            else if (mapColumnCompare < 0) return -1;
            else {
                if (mapTsCompare) return 1;
                else return -1;
            }
        } else if (BigTable.orderType == 5) {
            if (mapTsCompare) return 1;
            else return -1;
        } else if (BigTable.orderType == 9) {
            if (mapValueCompare > 0) {
                return 1;
            } else return -1;
        }
        if (mapRowCompare > 0) return 1;
        else if (mapRowCompare < 0) return -1;
        else if (mapColumnCompare > 0) return 1;
        else if (mapColumnCompare < 0) return -1;
        else {
            if (mapTsCompare) return 1;
            else return -1;
        }
    }
    
}



