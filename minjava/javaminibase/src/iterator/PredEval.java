package iterator;

import BigT.Map;
import heap.*;
import global.*;
import java.io.*;

public class PredEval
{
  /**
   *predicate evaluate, according to the condition ConExpr, judge if 
   *the two maps can join. if so, return true, otherwise false
   * @param p single select condition array
   * @param m1 compared map1
   * @param m2 compared map2
   *@return true or false
   *@exception IOException  some I/O error
   *@exception UnknowAttrType don't know the attribute type
   *@exception InvalidMapSizeException size of map not valid
   *@exception InvalidTypeException type of map not valid
   *@exception FieldNumberOutOfBoundException field number exceeds limit
   *@exception PredEvalException exception from this method
   */
  public static boolean Eval(CondExpr[] p, Map m1, Map m2)
    throws IOException,
	   UnknowAttrType,
          InvalidMapSizeException,
	   InvalidTypeException,
	   FieldNumberOutOfBoundException,
	   PredEvalException
    {
      CondExpr temp_ptr;
      int       i = 0;
      Map    map1 = null, map2 = null;
      int      fld1, fld2;
	  int fldNo = 0;
      Map    value =   new Map();
      short[]     str_size = {0, 0, 0, 0};
      //AttrType[]  val_type = new AttrType[1];
      
      AttrType  comparison_type = new AttrType(AttrType.attrInteger);
      int       comp_res;
      boolean   op_res = false, row_res = false, col_res = true;
      
      if (p == null)
	{
	  return true;
	}
      
      while (p[i] != null)
	{
	  temp_ptr = p[i];
	  while (temp_ptr != null)
	    {
	      //val_type[0] = new AttrType(temp_ptr.type1.attrType);
	      fld1        = 1;
	      switch (temp_ptr.type1.attrType)
		{
		case AttrType.attrInteger:
		  value.setHdr(null);
		  value.setTimeStamp(temp_ptr.operand1.integer);
		  map1 = value;
		  fldNo = 3;
		  //comparison_type.attrType = AttrType.attrInteger;
		  break;
		case AttrType.attrString:
			str_size[0] = (short)(temp_ptr.operand1.string.length()+1 );
			value.setHdr(str_size);
			value.setRowLabel(temp_ptr.operand1.string);
		    map1 = value;
			fldNo = 0;
		  //comparison_type.attrType = AttrType.attrString;
		  break;
		case AttrType.attrSymbol:
			fldNo = temp_ptr.operand1.symbol.offset - 1;
		  if (temp_ptr.operand1.symbol.relation.key == RelSpec.outer)
		    {
		      map1 = m1;
		      //comparison_type.attrType = map_fld_no1[fld1-1].attrType;
		    }
		  else
		    {
		      map1 = m2;
		      //comparison_type.attrType = map_fld_no2[fld1-1].attrType;
		    }
		  break;
		default:
		  break;
		}
	      
	      // Setup second argument for comparison.
	      //val_type[0] = new AttrType(temp_ptr.type2.attrType);
	      fld2        = 1;
	      switch (temp_ptr.type2.attrType)
		{
		case AttrType.attrInteger:
		  value.setHdr(null);
		  value.setTimeStamp(temp_ptr.operand2.integer);
		  map2 = value;
		  fldNo = 3;
		  break;
		case AttrType.attrString:
			str_size[0] = (short)(temp_ptr.operand2.string.length()+1);
			value.setHdr(str_size);
			value.setRowLabel(temp_ptr.operand2.string);
		    map2 = value;
			fldNo = 0;
		  break;
		case AttrType.attrSymbol:
			fldNo = temp_ptr.operand2.symbol.offset - 1;
		  if (temp_ptr.operand2.symbol.relation.key == RelSpec.outer)
		    map2 = m1;
		  else
		    map2 = m2;
		  break;
		default:
		  break;
		}
	      
	      
	      // Got the arguments, now perform a comparison.
	      try {
		comp_res = MapUtils.CompareMapWithMap(map1, map2, fldNo);
	      }catch (MapUtilsException e){
		throw new PredEvalException (e,"MapUtilsException is caught by PredEval.java");
	      }
	      op_res = false;
	      
	      switch (temp_ptr.op.attrOperator)
		{
		case AttrOperator.aopEQ:
		  if (comp_res == 0) op_res = true;
		  break;
		case AttrOperator.aopLT:
		  if (comp_res <  0) op_res = true;
		  break;
		case AttrOperator.aopGT:
		  if (comp_res >  0) op_res = true;
		  break;
		case AttrOperator.aopNE:
		  if (comp_res != 0) op_res = true;
		  break;
		case AttrOperator.aopLE:
		  if (comp_res <= 0) op_res = true;
		  break;
		case AttrOperator.aopGE:
		  if (comp_res >= 0) op_res = true;
		  break;
		case AttrOperator.aopNOT:
		  if (comp_res != 0) op_res = true;
		  break;
		default:
		  break;
		}
	      
	      row_res = row_res || op_res;
	      if (row_res == true)
		break;                        // OR predicates satisfied.
	      temp_ptr = temp_ptr.next;
	    }
	  i++;
	  
	  col_res = col_res && row_res;
	  if (col_res == false)
	    {
	      
	      return false;
	    }
	  row_res = false;                        // Starting next row.
	}
      
      
      return true;
      
    }
}

