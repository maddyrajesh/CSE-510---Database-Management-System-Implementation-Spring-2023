
package iterator;
import global.*;

import java.io.*;

/**
 * Implements a sorted binary tree.
 * abstract methods <code>enq</code> and <code>deq</code> are used to add 
 * or remove elements from the tree.
 */  
public abstract class pnodePQ
{
  /** number of elements in the tree */
  protected int                   count;

  /** the field number of the ordering type */
  protected int orderType;

  /** the attribute type of the sorting field */
  protected AttrType              fld_type;

  /** the sorting order (Ascending or Descending) */
  protected MapOrder sort_order;

  /**
   * class constructor, set <code>count</code> to <code>0</code>.
   */
  public pnodePQ() { count = 0; } 

  /**
   * returns the number of elements in the tree.
   * @return number of elements in the tree.
   */
  public int       length(){ return count; }  

  /** 
   * tests whether the tree is empty
   * @return true if tree is empty, false otherwise
   */
  public boolean   empty() { return count == 0; }
  

  /**
   * insert an element in the tree in the correct order.
   * @param item the element to be inserted
   * @exception IOException from lower layers
   * @exception UnknowAttrType <code>attrSymbol</code> or
   *                           <code>attrNull</code> encountered
   * @exception MapUtilsException error in map compare routines
 * @throws InvalidFieldNo
   */
  abstract public void  enq(pnode  item) 
           throws IOException, UnknowAttrType, MapUtilsException, InvalidFieldNo;

  /**
   * removes the minimum (Ascending) or maximum (Descending) element
   * from the tree.
   * @return the element removed, null if the tree is empty
   */
  abstract public pnode    deq();
	

  /**
   * compares two elements.
   * @param a one of the element for comparison
   * @param b the other element for comparison
   * @return  <code>0</code> if the two are equal,
   *          <code>1</code> if <code>a</code> is greater,
   *         <code>-1</code> if <code>b</code> is greater
   * @exception IOException from lower layers
   * @exception UnknowAttrType <code>attrSymbol</code> or 
   *                           <code>attrNull</code> encountered
   * @exception MapUtilsException error in map compare routines
   */
  public int pnodeCMP(pnode a, pnode b)
          throws IOException, UnknowAttrType, MapUtilsException, InvalidFieldNo {
    int ans = MapUtils.CompareMapsOnOrderType(a.map, b.map, orderType);
    return ans;
  }

  /**
   * tests whether the two elements are equal.
   * @param a one of the element for comparison
   * @param b the other element for comparison
   * @return <code>true</code> if <code>a == b</code>,
   *         <code>false</code> otherwise
   * @exception IOException from lower layers
   * @exception UnknowAttrType <code>attrSymbol</code> or 
   *                           <code>attrNull</code> encountered
   * @exception MapUtilsException error in map compare routines
   * @throws InvalidFieldNo
   */  
  public boolean pnodeEQ(pnode a, pnode b) throws IOException, UnknowAttrType, MapUtilsException, InvalidFieldNo {
    return pnodeCMP(a, b) == 0;
  }
  
  /**
   * tests whether the a is less than or equal to b
   * @param a one of the element for comparison
   * @param b the other element for comparison
   * @return <code>true</code> if <code>a <= b</code>,
   *         <code>false</code> otherwise
   * @exception IOException from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   * @exception MapUtilsException error in map compare routines
   */  
  /*
  public boolean pnodeLE(pnode a, pnode b)throws IOException, UnknowAttrType, MapUtilsException {
    if (sort_order.MapOrder == MapOrder.Ascending)
      return pnodeCMP(a, b) <= 0;
    else if (sort_order.mapOrder == MapOrder.Descending)
      return pnodeCMP(a, b) >= 0;
    else throw new UnknowAttrType("error in pnodePQ.java"); 
  }
  */
  /*
  virtual pnode&          front() = 0;             // access min item
  virtual void          del_front() = 0;         // delete min item

  virtual int           contains(pnode  item);     // is item in PQ?

  virtual void          clear();                 // delete all items

  virtual Pix           first() = 0;             // Pix of first item or 0
  virtual void          next(Pix& i) = 0;        // advance to next or 0
  virtual pnode&          operator () (Pix i) = 0; // access item at i
  virtual void          del(Pix i) = 0;          // delete item at i
  virtual int           owns(Pix i);             // is i a valid Pix  ?
  virtual Pix           seek(pnode  item);         // Pix of item

  void                  error(const char* msg);
  virtual int           OK() = 0;                // rep invariant
  */
}
