package index;
import BigT.Map;
import global.*;
import btree.*;
import iterator.*;
import heap.*; 
import java.io.*;


/**
 * Index Scan iterator will directly access the required map using
 * the provided key. It will also perform selections and projections.
 * information about the maps and the index are passed to the constructor,
 * then the user calls <code>get_next()</code> to get the maps.
 */
public class IndexScan extends Iterator {

  /**
   * class constructor. set up the index scan.
   * @param index type of the index (B_Index, Hash)
   * @param relName name of the input relation
   * @param indName name of the input index
   * @param types array of types in this relation
   * @param str_sizes array of string sizes (for attributes that are string)
   * @param noInFlds number of fields in input map
   * @param noOutFlds number of fields in output map
   * @param outFlds fields to project
   * @param selects conditions to apply, first one is primary
   * @param fldNum field number of the indexed field
   * @param indexOnly whether the answer requires only the key or the map
   * @exception IndexException error from the lower layer
   * @exception InvalidTypeException map type not valid
   * @exception InvalidMapSizeException map size not valid
   * @exception UnknownIndexTypeException index type unknown
   * @exception IOException from the lower layer
   * @throws InvalidRelation
   * @throws TupleUtilsException
   */
  public IndexScan(
	   IndexType     index,        
	   final String  relName,  
	   final String  indName,  
	   AttrType      types[],      
	   short         str_sizes[],     
	   int           noInFlds,          
	   int           noOutFlds,         
	   FldSpec       outFlds[],     
	   CondExpr      selects[],  
	   final int     fldNum,
	   final boolean indexOnly
	   )
          throws IndexException,
          InvalidTypeException,
          InvalidMapSizeException,
          UnknownIndexTypeException,
          IOException, InvalidRelation, TupleUtilsException, MapUtilsException {
    _fldNum = fldNum;
    _noInFlds = noInFlds;
    _types = types;
    _s_sizes = str_sizes;
    
    AttrType[] Jtypes = new AttrType[noOutFlds];
    short[] ts_sizes;
    Jmap = new Map();
    
    ts_sizes = MapUtils.setup_op_map(Jmap, Jtypes, types, noInFlds, str_sizes, outFlds, noOutFlds);
     
    _selects = selects;
    perm_mat = outFlds;
    _noOutFlds = noOutFlds;
    map1 = new Map();    
    try {
      map1.setHdr(str_sizes);
    }
    catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: Heapfile error");
    }
    
    t1_size = map1.size();
    index_only = indexOnly;  // added by bingjie miao
    
    try {
      f = new Heapfile(relName);
    }
    catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: Heapfile not created");
    }
    
    switch(index.indexType) {
      // linear hashing is not yet implemented
    case IndexType.B_Index:
      // error check the select condition
      // must be of the type: value op symbol || symbol op value
      // but not symbol op symbol || value op value
      try {
	indFile = new BTreeFile(indName); 
      }
      catch (Exception e) {
	throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
      }
      
      try {
	indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
      }
      catch (Exception e) {
	throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
      }
      
      break;
    case IndexType.None:
    default:
      throw new UnknownIndexTypeException("Only BTree index is supported so far");
      
    }
    
  }
  
  /**
   * returns the next map.
   * if <code>index_only</code>, only returns the key value
   * otherwise, retrive the map and returns the whole map
   * @return the map
   * @exception IndexException error from the lower layer
   * @exception UnknownKeyTypeException key type unknown
   * @exception IOException from the lower layer
   */
  public Map get_next()
    throws IndexException, 
	   UnknownKeyTypeException,
	   IOException
  {
    MID mid;
    int unused;
    KeyDataEntry nextentry = null;

    try {
      nextentry = indScan.get_next();
    }
    catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: BTree error");
    }	  
    
    while(nextentry != null) {
      if (index_only) {
	// only need to return the key 

	AttrType[] attrType = new AttrType[1];
	short[] s_sizes = new short[1];

	if (_fldNum - 1 == 0) {
	  attrType[0] = new AttrType(AttrType.attrString);
	  try {
	    Jmap.setHdr(s_sizes);
	  }
	  catch (Exception e) {
	    throw new IndexException(e, "IndexScan.java: Heapfile error");
	  }

	  try {
          Jmap.setRowLabel(((StringKey)nextentry.key).getKey());
	  }
	  catch (Exception e) {
	    throw new IndexException(e, "IndexScan.java: Heapfile error");
	  }
	}
    else if(_fldNum - 1 == 1) {
        attrType[0] = new AttrType(AttrType.attrString);
        try {
            Jmap.setHdr(s_sizes);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile error");
        }

        try {
            Jmap.setColumnLabel(((StringKey) nextentry.key).getKey());
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile error");
        }
    }
    else if(_fldNum - 1 == 2) {
        attrType[0] = new AttrType(AttrType.attrInteger);
        try {
            Jmap.setHdr(s_sizes);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile error");
        }

        try {
            Jmap.setTimeStamp(((IntegerKey) nextentry.key).getKey());
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile error");
        }
    }
    else if (_types[_fldNum -1].attrType == AttrType.attrString) {
	  
	  attrType[0] = new AttrType(AttrType.attrString);
	  // calculate string size of _fldNum
	  int count = 0;
	  for (int i=0; i<_fldNum; i++) {
	    if (_types[i].attrType == AttrType.attrString)
	      count ++;
	  } 
	  s_sizes[0] = _s_sizes[count-1];
	  
	  try {
	    Jmap.setHdr(s_sizes);
	  }
	  catch (Exception e) {
	    throw new IndexException(e, "IndexScan.java: Heapfile error");
	  }
	  
	  try {
	    Jmap.setValue(((StringKey)nextentry.key).getKey());
	  }
	  catch (Exception e) {
	    throw new IndexException(e, "IndexScan.java: Heapfile error");
	  }	  
	}
	else {
	  // attrReal not supported for now
	  throw new UnknownKeyTypeException("Only Integer and String keys are supported so far"); 
	}
	return Jmap;
      }
      
      // not index_only, need to return the whole map
      mid = ((LeafData)nextentry.data).getData();
      try {
	map1 = f.getMap(mid);
      }
      catch (Exception e) {
	throw new IndexException(e, "IndexScan.java: getRecord failed");
      }
      
      try {
	map1.setHdr(_s_sizes);
      }
      catch (Exception e) {
	throw new IndexException(e, "IndexScan.java: Heapfile error");
      }
    
      boolean eval;
      try {
	eval = PredEval.Eval(_selects, map1, Jmap);
      }
      catch (Exception e) {
	throw new IndexException(e, "IndexScan.java: Heapfile error");
      }
      
      if (eval) {
	// need projection.java
	try {
	  Projection.Project(map1, _types, Jmap, perm_mat, _noOutFlds);
	}
	catch (Exception e) {
	  throw new IndexException(e, "IndexScan.java: Heapfile error");
	}

	return Jmap;
      }

      try {
	nextentry = indScan.get_next();
      }
      catch (Exception e) {
	throw new IndexException(e, "IndexScan.java: BTree error");
      }	  
    }
    
    return null; 
  }
  
  /**
   * Cleaning up the index scan, does not remove either the original
   * relation or the index from the database.
   * @exception IndexException error from the lower layer
   * @exception IOException from the lower layer
   */
  public void close() throws IOException, IndexException
  {
    if (!closeFlag) {
      if (indScan instanceof BTFileScan) {
	try {
	  ((BTFileScan)indScan).DestroyBTreeFileScan();
	}
	catch(Exception e) {
	  throw new IndexException(e, "BTree error in destroying index scan.");
	}
      }
      
      closeFlag = true; 
    }
  }
  
  public FldSpec[]      perm_mat;
  private IndexFile     indFile;
  private IndexFileScan indScan;
  private AttrType[]    _types;
  private short[]       _s_sizes; 
  private CondExpr[]    _selects;
  private int           _noInFlds;
  private int           _noOutFlds;
  private Heapfile      f;
  private Map         map1;
  private Map         Jmap;
  private int           t1_size;
  private int           _fldNum;       
  private boolean       index_only;    

}

