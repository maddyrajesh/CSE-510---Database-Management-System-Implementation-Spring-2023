package iterator;
   

import BigT.Map;
import heap.*;
import global.*;
import bufmgr.*;


import java.io.*;

/**
 *open a heapfile and according to the condition expression to get
 *output file, call get_next to get all maps
 */
public class FileScan extends MapIterator
{
  private AttrType[] _in1;
  private short in1_len;
  private short[] s_sizes; 
  private Heapfile f;
  private Scan scan;
  private Map map1;
  private Map Jmap;
  private int _nOutFlds;
  private CondExpr[]  OutputFilter;
  public FldSpec[] perm_mat;


    /**
     * constructor
     *
     * @param file_name  heapfile to be opened
     * @param in1        the in 1
     * @param s1_sizes   the s 1 sizes
     * @param len_in1    the len in 1
     * @param n_out_flds the n out flds
     * @param proj_list  shows what input fields go where in the output map
     * @param outFilter  select expressions
     * @throws IOException         the io exception
     * @throws FileScanException   the file scan exception
     * @throws InvalidRelation     the invalid relation
     * @throws MapUtilsException   the map utils exception
     * @throws TupleUtilsException the tuple utils exception
     */
    public  FileScan(String file_name,
                   AttrType[] in1,
                   short[] s1_sizes,
                   short len_in1,
                   int n_out_flds,
                   FldSpec[] proj_list,
                   CondExpr[] outFilter)
          throws IOException, FileScanException, InvalidRelation, MapUtilsException, TupleUtilsException
    {
      s_sizes = s1_sizes;
      _in1 = in1;
      in1_len = len_in1;
      _nOutFlds = n_out_flds;
      
      Jmap =  new Map();
      short[]    ts_size;
      AttrType[] Jtypes = new AttrType[n_out_flds];
      // Might need to change based on the MapUtils development.
      ts_size = MapUtils.setup_op_map(Jmap, Jtypes, _in1, in1_len, s1_sizes, proj_list, n_out_flds);
      
      OutputFilter = outFilter;
      perm_mat = proj_list;
      map1 =  new Map();

      try {
	map1.setHdr(s1_sizes);
      }catch (Exception e){
	throw new FileScanException(e, "setHdr() failed");
      }
      map1.size();
      
      try {
	f = new Heapfile(file_name);
	
      }
      catch(Exception e) {
	throw new FileScanException(e, "Create new heapfile failed");
      }
      
      try {
	scan = f.openScan();
      }
      catch(Exception e){
	throw new FileScanException(e, "openScan() failed");
      }
    }
  
  /**
   *@return shows what input fields go where in the output map
   */
  public FldSpec[] show()
    {
      return perm_mat;
    }
  
  /**
   *@return the result map
   *@exception JoinsException some join exception
   *@exception IOException I/O errors
   *@exception InvalidMapSizeException invalid map size
   *@exception InvalidTypeException map type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception PredEvalException exception from PredEval class
   *@exception UnknowAttrType attribute type unknown
   *@exception FieldNumberOutOfBoundException array out of bounds
   *@exception WrongPermat exception for wrong FldSpec argument
   * @throws InvalidFieldNo
   */
  public Map get_next()
          throws JoinsException,
          IOException,
          InvalidMapSizeException,
          InvalidTypeException,
          PageNotReadException,
          PredEvalException,
          UnknowAttrType,
          FieldNumberOutOfBoundException,
          WrongPermat, InvalidFieldNo, MapUtilsException {
      MID mid = new MID();
      
      while(true) {
	if((map1 =  scan.getNext(mid)) == null) {
	  return null;
	}
    //System.out.println("found matching map! row is: " + map1.getRowLabel() + " column is: " + map1.getColumnLabel()  + " time is: " + map1.getTimeStamp() + " val is: " + map1.getValue());


          //map1.setHeader();dr(s_sizes);
	if (PredEval.Eval(OutputFilter, map1, null) == true){
        //System.out.println("found eval: " + map1.getTimeStamp());
	  Projection.Project(map1, _in1, Jmap, perm_mat, _nOutFlds);
	  return Jmap;
	}        
      }
    }

  /**
   *implement the abstract method close() from super class Iterator
   *to finish cleaning up
   */
  public void close() 
    {
     
      if (!closeFlag) {
	scan.closescan();
	closeFlag = true;
      } 
    }
  
}


