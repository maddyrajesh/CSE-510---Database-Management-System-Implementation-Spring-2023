package iterator;

import BigT.Map;
import global.AttrType;
import global.GlobalConst;
import global.MapOrder;
import global.PageId;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;

import java.io.*;

/**
 * The Sort class sorts a file. All necessary information are passed as 
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get tuples in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class Sort extends Iterator implements GlobalConst
{
  private static final int ARBIT_RUNS = 10;
  private short map_size;
  private short[]     str_fld_lens=null;
  private int num_cols = 4;
  private static short REC_LEN1 = 32;
  private Iterator    map_iter;
  private int         _sort_fld;
  private MapOrder  map_order;
  private int         _n_pages;
  private byte[][]    bufs;
  private boolean     first_time;
  private int         max_elems_in_heap;
  private int         sortFldLen;
  private pnodeSplayPQ Q;
  private Heapfile[]   temp_files; 
  private int          n_tempfiles;
  private Map        output_map;
  private int[]        n_maps;
  private int          n_runs;
  private Map        op_map_buf;
  private OBuf         o_buf;
  private SpoofIbuf[]  i_buf;
  private PageId[]     bufs_pids;
  AttrType[] mapAttributes = new AttrType[4];
  private boolean useBM = true; // flag for whether to use buffer manager

  /**
   * Set up for merging the runs.
   * Open an input buffer for each run, and insert the first element (min)
   * from each run into a heap. <code>delete_min() </code> will then get 
   * the minimum of all runs.
   * @param map_size size (in bytes) of each map
   * @param n_R_runs number of runs
   * @exception IOException from lower layers
   * @exception LowMemException there is not enough memory to 
   *                 sort in two passes (a subclass of SortException).
   * @exception SortException something went wrong in the lower layer. 
   * @exception Exception other exceptions
   */
  private void setup_for_merge(int map_size, int n_R_runs)
    throws IOException,
	   LowMemException,
	   SortException,
	   Exception
  {
    // don't know what will happen if n_R_runs > _n_pages
    if (n_R_runs > _n_pages) 
      throw new LowMemException("Sort.java: Not enough memory to sort in two passes."); 

    int i;
    pnode cur_node;  // need pq_defs.java
    
    i_buf = new SpoofIbuf[n_R_runs];   // need io_bufs.java
    for (int j=0; j<n_R_runs; j++) i_buf[j] = new SpoofIbuf();
    
    // construct the lists, ignore TEST for now
    // this is a patch, I am not sure whether it works well -- bingjie 4/20/98
    
    for (i=0; i<n_R_runs; i++) {
      byte[][] apage = new byte[1][];
      apage[0] = bufs[i];

      // need iobufs.java
      i_buf[i].init(temp_files[i], apage, 1, map_size, n_maps[i]);

      cur_node = new pnode();
      cur_node.run_num = i;
      
      // may need change depending on whether Get() returns the original
      // or make a copy of the tuple, need io_bufs.java ???
      Map temp_map = new Map();

      try {
	temp_map.setHdr(str_fld_lens);
      }
      catch (Exception e) {
	throw new SortException(e, "Sort.java: Tuple.setHdr() failed");
      }
      
      temp_map =i_buf[i].Get(temp_map);  // need implement iterator.SpoofIbuf
            
      if (temp_map != null) {
	/*
	System.out.print("Get tuple from run " + i);
	temp_tuple.print(_in);
	*/
	cur_node.map = temp_map; // no copy needed
	try {
	  Q.enq(cur_node);
	}
	catch (UnknowAttrType e) {
	  throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
	}
	catch (MapUtilsException e) {
	  throw new SortException(e, "Sort.java: MapUtilsException caught from Q.enq()");
	}

      }
    }
    return; 
  }
  
  /**
   * Generate sorted runs.
   * Using heap sort.
   * @param  max_elems    maximum number of elements in heap
   * @param  sortFldType  attribute type of the sort field
   * @param  sortFldLen   length of the sort field
   * @return number of runs generated
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   * @exception JoinsException from <code>Iterator.get_next()</code>
   */
  private int generate_runs(int max_elems, AttrType sortFldType, int sortFldLen) 
    throws IOException, 
	   SortException, 
	   UnknowAttrType,
	   TupleUtilsException,
	   JoinsException,
	   Exception
  {
    Map map;
    pnode cur_node;
    pnodeSplayPQ Q1 = new pnodeSplayPQ(_sort_fld, sortFldType, map_order);
    pnodeSplayPQ Q2 = new pnodeSplayPQ(_sort_fld, sortFldType, map_order);
    pnodeSplayPQ pcurr_Q = Q1;
    pnodeSplayPQ pother_Q = Q2; 
    Map lastElem = new Map();  // need tuple.java
    try {
      lastElem.setHdr(str_fld_lens);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: setHdr() failed");
    }
    
    int run_num = 0;  // keeps track of run_num

    // number of elements in Q
    int p_elems_curr_Q = 0;
    int p_elems_other_Q = 0;
    
    int comp_res;
    
    // set the lastElem to be the min value for the sort field
    if(map_order.mapOrder == MapOrder.Ascending) {
      try {
	MIN_VAL(lastElem, sortFldType);
      } catch (UnknowAttrType e) {
	throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
      } catch (Exception e) {
	throw new SortException(e, "MIN_VAL failed");
      } 
    }
    else {
      try {
	MAX_VAL(lastElem, sortFldType);
      } catch (UnknowAttrType e) {
	throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
      } catch (Exception e) {
	throw new SortException(e, "MAX_VAL failed");
      } 
    }
    
    // maintain a fixed maximum number of elements in the heap
    while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
      try {
	map = map_iter.get_next();
      } catch (Exception e) {
	e.printStackTrace(); 
	throw new SortException(e, "Sort.java: get_next() failed");
      } 
      
      if (map == null) {
	break;
      }
      cur_node = new pnode();
      cur_node.map = new Map(map); // tuple copy needed --  Bingjie 4/29/98

      pcurr_Q.enq(cur_node);
      p_elems_curr_Q ++;
    }
    
    // now the queue is full, starting writing to file while keep trying
    // to add new tuples to the queue. The ones that does not fit are put
    // on the other queue temperarily
    while (true) {
      cur_node = pcurr_Q.deq();
      if (cur_node == null) break; 
      p_elems_curr_Q --;
      
      comp_res = MapUtils.CompareMapWithValue(cur_node.map, _sort_fld, lastElem);
      
      if ((comp_res < 0 && map_order.mapOrder == MapOrder.Ascending) || (comp_res > 0 && map_order.mapOrder == MapOrder.Descending)) {
	// doesn't fit in current run, put into the other queue
	try {
	  pother_Q.enq(cur_node);
	}
	catch (UnknowAttrType e) {
	  throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
	}
	p_elems_other_Q ++;
      }
      else {
	// set lastElem to have the value of the current tuple,
	// need tuple_utils.java
	MapUtils.SetValue(lastElem, cur_node.map, _sort_fld);
	// write tuple to output file, need io_bufs.java, type cast???
	//	System.out.println("Putting tuple into run " + (run_num + 1)); 
	//	cur_node.tuple.print(_in);
	
	o_buf.Put(cur_node.map);
      }
      
      // check whether the other queue is full
      if (p_elems_other_Q == max_elems) {
	// close current run and start next run
	n_maps[run_num] = (int) o_buf.flush();  // need io_bufs.java
	run_num ++;

	// check to see whether need to expand the array
	if (run_num == n_tempfiles) {
	  Heapfile[] temp1 = new Heapfile[2*n_tempfiles];
	  for (int i=0; i<n_tempfiles; i++) {
	    temp1[i] = temp_files[i];
	  }
	  temp_files = temp1; 
	  n_tempfiles *= 2; 

	  int[] temp2 = new int[2*n_runs];
	  for(int j=0; j<n_runs; j++) {
	    temp2[j] = n_maps[j];
	  }
	  n_maps = temp2;
	  n_runs *=2; 
	}
	
	try {
	    temp_files[run_num] = new Heapfile(null);
	}
	catch (Exception e) {
	  throw new SortException(e, "Sort.java: create Heapfile failed");
	}
	
	// need io_bufs.java
	o_buf.init(bufs, _n_pages, map_size, temp_files[run_num], false);
	
	// set the last Elem to be the minimum value for the sort field
	if(map_order.mapOrder == MapOrder.Ascending) {
	  try {
	    MIN_VAL(lastElem, sortFldType);
	  } catch (UnknowAttrType e) {
	    throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
	  } catch (Exception e) {
	    throw new SortException(e, "MIN_VAL failed");
	  } 
	}
	else {
	  try {
	    MAX_VAL(lastElem, sortFldType);
	  } catch (UnknowAttrType e) {
	    throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
	  } catch (Exception e) {
	    throw new SortException(e, "MIN_VAL failed");
	  } 
	}
    
	// switch the current heap and the other heap
	pnodeSplayPQ tempQ = pcurr_Q;
	pcurr_Q = pother_Q;
	pother_Q = tempQ;
	int tempelems = p_elems_curr_Q;
	p_elems_curr_Q = p_elems_other_Q;
	p_elems_other_Q = tempelems;
      }
      
      // now check whether the current queue is empty
      else if (p_elems_curr_Q == 0) {
	while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
	  try {
	    map = map_iter.get_next();  // according to Iterator.java
	  } catch (Exception e) {
	    throw new SortException(e, "get_next() failed");
	  } 
	  
	  if (map == null) {
	    break;
	  }
	  cur_node = new pnode();
	  cur_node.map = new Map(map); // tuple copy needed --  Bingjie 4/29/98

	  try {
	    pcurr_Q.enq(cur_node);
	  }
	  catch (UnknowAttrType e) {
	    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
	  }
	  p_elems_curr_Q ++;
	}
      }
      
      // Check if we are done
      if (p_elems_curr_Q == 0) {
	// current queue empty despite our attemps to fill in
	// indicating no more tuples from input
	if (p_elems_other_Q == 0) {
	  // other queue is also empty, no more tuples to write out, done
	  break; // of the while(true) loop
	}
	else {
	  // generate one more run for all tuples in the other queue
	  // close current run and start next run
	  n_maps[run_num] = (int) o_buf.flush();  // need io_bufs.java
	  run_num ++;
	  
	  // check to see whether need to expand the array
	  if (run_num == n_tempfiles) {
	    Heapfile[] temp1 = new Heapfile[2*n_tempfiles];
	    for (int i=0; i<n_tempfiles; i++) {
	      temp1[i] = temp_files[i];
	    }
	    temp_files = temp1; 
	    n_tempfiles *= 2; 
	    
	    int[] temp2 = new int[2*n_runs];
	    for(int j=0; j<n_runs; j++) {
	      temp2[j] = n_maps[j];
	    }
	    n_maps = temp2;
	    n_runs *=2; 
	  }

	  try {
	    temp_files[run_num] = new Heapfile(null); 
	  }
	  catch (Exception e) {
	    throw new SortException(e, "Sort.java: create Heapfile failed");
	  }
	  
	  // need io_bufs.java
	  o_buf.init(bufs, _n_pages, map_size, temp_files[run_num], false);
	  
	  // set the last Elem to be the minimum value for the sort field
	  if(map_order.mapOrder == MapOrder.Ascending) {
	    try {
	      MIN_VAL(lastElem, sortFldType);
	    } catch (UnknowAttrType e) {
	      throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
	    } catch (Exception e) {
	      throw new SortException(e, "MIN_VAL failed");
	    } 
	  }
	  else {
	    try {
	      MAX_VAL(lastElem, sortFldType);
	    } catch (UnknowAttrType e) {
	      throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
	    } catch (Exception e) {
	      throw new SortException(e, "MAX_VAL failed");
	    } 
	  }
	
	  // switch the current heap and the other heap
	  pnodeSplayPQ tempQ = pcurr_Q;
	  pcurr_Q = pother_Q;
	  pother_Q = tempQ;
	  int tempelems = p_elems_curr_Q;
	  p_elems_curr_Q = p_elems_other_Q;
	  p_elems_other_Q = tempelems;
	}
      } // end of if (p_elems_curr_Q == 0)
    } // end of while (true)

    // close the last run
    n_maps[run_num] = (int) o_buf.flush();
    run_num ++;
    
    return run_num; 
  }
  
  /**
   * Remove the minimum value among all the runs.
   * @return the minimum tuple removed
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */
  private Map delete_min()
    throws IOException, 
	   SortException,
	   Exception
  {
    pnode cur_node;                // needs pq_defs.java  
    Map new_map, old_map;

    cur_node = Q.deq();
    old_map = cur_node.map;
    /*
    System.out.print("Get ");
    old_tuple.print(_in);
    */
    // we just removed one tuple from one run, now we need to put another
    // tuple of the same run into the queue
    if (!i_buf[cur_node.run_num].empty()) {
      // run not exhausted 
      new_map = new Map(); // need tuple.java??

      try {
	new_map.setHdr(str_fld_lens); // Not sure about the input
      }
      catch (Exception e) {
	throw new SortException(e, "Sort.java: setHdr() failed");
      }
      
      new_map = i_buf[cur_node.run_num].Get(new_map);
      if (new_map != null) {
	/*
	System.out.print(" fill in from run " + cur_node.run_num);
	new_tuple.print(_in);
	*/
	cur_node.map = new_map;  // no copy needed -- I think Bingjie 4/22/98
	try {
	  Q.enq(cur_node);
	} catch (UnknowAttrType e) {
	  throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
	} catch (MapUtilsException e) {
	  throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
	} 
      }
      else {
	throw new SortException("********** Wait a minute, I thought input is not empty ***************");
      }
      
    }

    // changed to return Tuple instead of return char array ????
    return old_map;
  }
  
  /**
   * Set lastElem to be the minimum value of the appropriate type
   * @param lastElem the tuple
   * @param sortFldType the sort field type
   * @exception IOException from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   */
  private void MIN_VAL(Map lastElem, AttrType sortFldType)
    throws IOException, 
	   FieldNumberOutOfBoundException,
	   UnknowAttrType {

    //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
    //    AttrType[] junk = new AttrType[1];
    //    junk[0] = new AttrType(sortFldType.attrType);
    char[] c = new char[1];
    c[0] = Character.MIN_VALUE; 
    String s = new String(c);
    //    short fld_no = 1;
    
    switch (sortFldType.attrType) {
    case AttrType.attrInteger: 
      //      lastElem.setHdr(fld_no, junk, null);
      lastElem.setTimeStamp(Integer.MIN_VALUE);
      break;
    case AttrType.attrString:
        if (_sort_fld == 1)
            lastElem.setRowLabel(s);
        else if (_sort_fld == 2)
            lastElem.setColumnLabel(s);
        else if (_sort_fld == 4)
            lastElem.setValue(s);
      break;
    default:
      // don't know how to handle attrSymbol, attrNull
      //System.err.println("error in sort.java");
      throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }
    
    return;
  }

  /**
   * Set lastElem to be the maximum value of the appropriate type
   * @param lastElem the tuple
   * @param sortFldType the sort field type
   * @exception IOException from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   */
  private void MAX_VAL(Map lastElem, AttrType sortFldType)
    throws IOException, 
	   FieldNumberOutOfBoundException,
	   UnknowAttrType {

    //    short[] s_size = new short[Tuple.max_size]; // need Tuple.java
    //    AttrType[] junk = new AttrType[1];
    //    junk[0] = new AttrType(sortFldType.attrType);
    char[] c = new char[1];
    c[0] = Character.MAX_VALUE; 
    String s = new String(c);
    //    short fld_no = 1;
    
    switch (sortFldType.attrType) {
    case AttrType.attrInteger: 
      //      lastElem.setHdr(fld_no, junk, null);
        lastElem.setTimeStamp(Integer.MAX_VALUE);
      break;
    case AttrType.attrString:
        if (_sort_fld == 1)
            lastElem.setRowLabel(s);
        else if (_sort_fld == 2)
            lastElem.setColumnLabel(s);
        else if (_sort_fld == 4)
            lastElem.setValue(s);
        break;
    default:
      // don't know how to handle attrSymbol, attrNull
      //System.err.println("error in sort.java");
      throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }
    
    return;
  }
  
  /** 
   * Class constructor, take information about the tuples, and set up 
   * the sorting
   * @param in array containing attribute types of the relation
   * @param len_in number of columns in the relation
   * @param str_sizes array of sizes of string attributes
   * @param am an iterator for accessing the tuples
   * @param sort_fld the field number of the field to sort on
   * @param sort_order the sorting order (ASCENDING, DESCENDING)
   * @param sort_field_len the length of the sort field
   * @param n_pages amount of memory (in pages) available for sorting
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */
  public Sort(AttrType[] attrTypes,
              short[] fld_sizes,
              Iterator map_iter,
              int sort_fld,
              MapOrder map_order,
              int n_pages,
              int sortFldlen,
              boolean mapInsertOrder)
          throws SortException {
    int str_att_count = 0; // number of string field in maps
    for (int i = 0; i < num_cols; i++) {
        mapAttributes[i] = new AttrType(attrTypes[i].attrType);
        if (attrTypes[i].attrType == AttrType.attrString) {
            // check if attribute is a string attribute and count them
            str_att_count++; // ideally should be 3
        }
    }


    str_fld_lens = new short[str_att_count];

    str_att_count = 0;
    for (int i = 0; i < num_cols; i++) {
        if (mapAttributes[i].attrType == AttrType.attrString) {
            str_fld_lens[str_att_count] = fld_sizes[str_att_count]; // will be RECLEN1 always
            str_att_count++;
        }
    }

    //-----------------------------------------------------------------------------------------------------------//

    Map tempMap = new Map();

    try {
        tempMap.setHdr(str_fld_lens);
    } catch (Exception e) {
        throw new SortException(e, "Sort.java: t.setHdr() failed");
    }
    map_size = tempMap.size();
    //Iterator mapIterObj = map_iter; //iterator passed to the sort object.
    _sort_fld = sort_fld;
    //MapOrder mapOrder = map_order;
    //int _num_pages = n_pages; //memory available for sorting in terms of pages
    bufs_pids = new PageId[n_pages];
    bufs = new byte[n_pages][];
    try {
        get_buffer_pages(n_pages, bufs_pids, bufs);
    } catch (IteratorBMException e) {
        e.printStackTrace();
    }
    first_time = true;


//        for (int k = 0; k < _num_pages; k++) bufs[k] = new byte[MAX_SPACE];


    // as a heuristic, we set the number of runs to an arbitrary value
    // of ARBIT_RUNS
    temp_files = new Heapfile[ARBIT_RUNS];
    n_tempfiles = ARBIT_RUNS;
    n_maps = new int[ARBIT_RUNS];
    n_runs = ARBIT_RUNS;

    try {
        temp_files[0] = new Heapfile(null);
    } catch (Exception e) {
        throw new SortException(e, "Sort.java: Heapfile error");
    }

    // just created temp heap files -----------------------------------------------------------------------------//

    o_buf = new OBuf(); //output buffer

    o_buf.init(bufs, _n_pages, map_size, temp_files[0], false);

    max_elems_in_heap = 5000;
    sortFldLen = sortFldlen;

    //pnodeSplayPQ queue = new pnodeSplayPQ(sort_fld, attrTypes[sort_fld - 1], map_order);

    try {
        op_map_buf = new Map(tempMap);
        op_map_buf.setHdr(str_fld_lens);
    } catch (Exception e) {
        throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
    }
  }

    /**
   * Returns the next tuple in sorted order.
   * Note: You need to copy out the content of the tuple, otherwise it
   *       will be overwritten by the next <code>get_next()</code> call.
   * @return the next tuple, null if all tuples exhausted
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   * @exception JoinsException from <code>generate_runs()</code>.
   * @exception UnknowAttrType attribute type unknown
   * @exception LowMemException memory low exception
   * @exception Exception other exceptions
   */
  public Map get_next()
    throws IOException, 
	   SortException, 
	   UnknowAttrType,
	   LowMemException, 
	   JoinsException,
	   Exception
  {
    if (first_time) {
      // first get_next call to the sort routine
      first_time = false;
      
      // generate runs
      n_runs = generate_runs(max_elems_in_heap, mapAttributes[_sort_fld-1], sortFldLen);
      //      System.out.println("Generated " + Nruns + " runs");
      
      // setup state to perform merge of runs. 
      // Open input buffers for all the input file
      setup_for_merge(map_size, n_runs);
    }
    
    if (Q.empty()) {  
      // no more tuples availble
      return null;
    }
    
    output_map = delete_min();
    if (output_map != null){
      op_map_buf.mapCopy(output_map);
      return op_map_buf;
    }
    else 
      return null; 
  }

  /**
   * Cleaning up, including releasing buffer pages from the buffer pool
   * and removing temporary files from the database.
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */
  public void close() throws SortException, IOException
  {
    // clean up
    if (!closeFlag) {
       
      try {
	map_iter.close();
      }
      catch (Exception e) {
	throw new SortException(e, "Sort.java: error in closing iterator.");
      }

      if (useBM) {
	try {
	  free_buffer_pages(_n_pages, bufs_pids);
	} 
	catch (Exception e) {
	  throw new SortException(e, "Sort.java: BUFmgr error");
	}
	for (int i=0; i<_n_pages; i++) bufs_pids[i].pid = INVALID_PAGE;
      }
      
      for (int i = 0; i<temp_files.length; i++) {
        if (temp_files[i] != null) {
          try {
            temp_files[i].deleteFile();
          }
          catch (Exception e) {
            throw new SortException(e, "Sort.java: Heapfile error");
          }
          temp_files[i] = null;
        }
      }
      closeFlag = true;
    } 
  } 

}


