/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */

package btree;
import java.io.*;
import java.lang.*;
import global.*;
import diskmgr.*;

/**
 * A BTLeafPage is a leaf page on a B+ tree.  It holds abstract 
 * <key, MID> pairs; it doesn't know anything about the keys
 * (their lengths or their types), instead relying on the abstract
 * interface consisting of BT.java.
 */
public class BTLeafPage extends BTSortedPage {
  
  /** pin the page with pageno, and get the corresponding BTLeafPage,
   * also it sets the type to be NodeType.LEAF.
   *@param pageno Input parameter. To specify which page number the
   *  BTLeafPage will correspond to.
   *@param keyType either AttrType.attrInteger or AttrType.attrString.
   *    Input parameter.   
   *@exception IOException  error from the lower layer
   *@exception ConstructPageException BTLeafPage constructor error
   */
  public BTLeafPage(PageId pageno, int keyType) 
    throws IOException, 
	   ConstructPageException
    {
      super(pageno, keyType);
      setType(NodeType.LEAF);
    }
  
  /**associate the BTLeafPage instance with the Page instance,
   * also it sets the type to be NodeType.LEAF. 
   *@param page  input parameter. To specify which page  the
   *  BTLeafPage will correspond to.
   *@param keyType either AttrType.attrInteger or AttrType.attrString.
   *  Input parameter.    
   *@exception IOException  error from the lower layer
   *@exception ConstructPageException BTLeafPage constructor error
   */
  public BTLeafPage(Page page, int keyType) 
    throws IOException, 
	   ConstructPageException
    {
      super(page, keyType);
      setType(NodeType.LEAF);
    }  
  
  /**new a page, associate the BTLeafPage instance with the Page instance,
   * also it sets the type to be NodeType.LEAF. 
   *@param keyType either AttrType.attrInteger or AttrType.attrString.
   *  Input parameter.
   *@exception IOException  error from the lower layer
   *@exception ConstructPageException BTLeafPage constructor error
   */
  public BTLeafPage( int keyType) 
    throws IOException, 
	   ConstructPageException
    {
      super(keyType);
      setType(NodeType.LEAF);
    }  
  

  
  /** insertRecord
   * READ THIS DESCRIPTION CAREFULLY. THERE ARE TWO MIDs
   * WHICH MEAN TWO DIFFERENT THINGS.
   * Inserts a key, mid value into the leaf node. This is
   * accomplished by a call to SortedPage::insertRecord()
   *  Parameters:
   *
   *@return - the mid of the inserted leaf record data entry,
   *           i.e., the <key, dataMid> pair.
   *@exception  LeafInsertRecException error when insert
   * @param key - the key value of the data record. Input parameter.
   * @param dataMid - the mid of the data record. This is
 *               stored on the leaf page along with the
   */   
  public MID insertRecord(KeyClass key, MID dataMid)
    throws  LeafInsertRecException
    {
      KeyDataEntry entry;
      
      try {
        entry = new KeyDataEntry( key, dataMid);
	
        return insertRecord(entry);
      }
      catch(Exception e) {
        throw new LeafInsertRecException(e, "insert record failed");
      }
    } // end of insertRecord
  
  
  /**  Iterators. 
   * One of the two functions: getFirst and getNext
   * which  provide an iterator interface to the records on a BTLeafPage.
   *@return return the first KeyDataEntry in the leaf page.
   * null if no more record
   *@exception  IteratorException iterator error
   * @param mid It will be modified and the first mid in the leaf page
   */
  public KeyDataEntry getFirst(MID mid)
    throws  IteratorException
    {
      
      KeyDataEntry  entry; 
      
      try {
        mid.pageNo = getCurPage();
        mid.slotNo = 0; // begin with first slot
	
        if ( getSlotCnt() <= 0) {
          return null;
        }

        entry=BT.getEntryFromBytes(getpage(), getSlotOffset(0), getSlotLength(0),
				   keyType, NodeType.LEAF);
	
        return entry;
      }
      catch (Exception e) {
	throw new IteratorException(e, "Get first entry failed");
      }
    } // end of getFirst

 
   /**Iterators.  
    * One of the two functions: getFirst and getNext which  provide an
    * iterator interface to the records on a BTLeafPage.
    *@return return the next KeyDataEntry in the leaf page. 
    *null if no more record.
    *@exception IteratorException iterator error
    * @param mid It will be modified and the next mid will be passed out
    */

   public KeyDataEntry getNext (MID mid)
     throws  IteratorException
   {
     KeyDataEntry  entry; 
     int i;
     try{
       mid.slotNo++; //must before any return;
       i= mid.slotNo;
       
       if ( mid.slotNo >= getSlotCnt())
       {
	 return null;
       }
       
       entry=BT.getEntryFromBytes(getpage(),getSlotOffset(i), getSlotLength(i),
                  keyType, NodeType.LEAF);
       
       return entry;
     } 
     catch (Exception e) {
       throw new IteratorException(e,"Get next entry failed");
     }
  }
  
  
  
  /**
   * getCurrent returns the current record in the iteration; it is like
   * getNext except it does not advance the iterator.
   *@return return the current KeyDataEntry
   *@exception  IteratorException iterator error
   * @param mid  the current mid. Input and Output parameter. But
   */ 
   public KeyDataEntry getCurrent (MID mid)
       throws  IteratorException
   {  
     mid.slotNo--;
     return getNext(mid);
   }
  
  
  /** 
   * delete a data entry in the leaf page.
   *@param dEntry the entry will be deleted in the leaf page. Input parameter.
   *@return true if deleted; false if no dEntry in the page
   *@exception LeafDeleteException error when delete
   */
   public boolean delEntry (KeyDataEntry dEntry)
     throws  LeafDeleteException
    {
      KeyDataEntry  entry;
      MID mid=new MID();
      
      try {
	for(entry = getFirst(mid); entry!=null; entry=getNext(mid))
	  {  
	    if ( entry.equals(dEntry) ) {
	      if ( super.deleteSortedRecord( mid ) == false )
		throw new LeafDeleteException(null, "Delete record failed");
	      return true;
	    }
	    
	 }
	return false;
      } 
      catch (Exception e) {
	throw new LeafDeleteException(e, "delete entry failed");
      }
      
    } // end of delEntry

  /*used in full delete 
   *@param leafPage the sibling page of this. Input parameter.
   *@param parentIndexPage the parant of leafPage and this. Input parameter.
   *@param direction -1 if "this" is left sibling of leafPage ; 
   *      1 if "this" is right sibling of leafPage. Input parameter.
   *@param deletedKey the key which was already deleted, and cause 
   *        redistribution. Input parameter.
   *@exception LeafRedistributeException
   *@return true if redistrbution success. false if we can not redistribute them.
   */
  boolean redistribute(BTLeafPage leafPage, BTIndexPage parentIndexPage, 
		       int direction, KeyClass deletedKey)
    throws LeafRedistributeException
    {
      boolean st;
      // assertion: leafPage pinned
      try {
	if (direction ==-1) { // 'this' is the left sibling of leafPage
	  if ( (getSlotLength(getSlotCnt()-1) + available_space()+ 8 /*  2*sizeof(slot) */) > 
	       ((MAX_SPACE-DPFIXED)/2)) {
            // cannot spare a record for its underflow sibling
            return false;
	  }
	  else {
            // move the last record to its sibling
	    
            // get the last record 
            KeyDataEntry lastEntry;
            lastEntry=BT.getEntryFromBytes(getpage(),getSlotOffset(getSlotCnt()-1)
					   ,getSlotLength(getSlotCnt()-1), keyType, NodeType.LEAF);
	    
	    
            //get its sibling's first record's key for adjusting parent pointer
            MID dummyMid=new MID();
            KeyDataEntry firstEntry;
            firstEntry=leafPage.getFirst(dummyMid);

            // insert it into its sibling            
            leafPage.insertRecord(lastEntry);
            
            // delete the last record from the old page
            MID delMid=new MID();
            delMid.pageNo = getCurPage();
            delMid.slotNo = getSlotCnt()-1;
            if ( deleteSortedRecord(delMid) == false )
	      throw new LeafRedistributeException(null, "delete record failed");

	    
            // adjust the entry pointing to sibling in its parent
            if (deletedKey != null)
                st = parentIndexPage.adjustKey(lastEntry.key, deletedKey);
            else 
                st = parentIndexPage.adjustKey(lastEntry.key,
                                            firstEntry.key);
            if (st == false) 
	      throw new LeafRedistributeException(null, "adjust key failed");
            return true;
	  }
	}
	else { // 'this' is the right sibling of pptr
	  if ( (getSlotLength(0) + available_space()+ 8) > ((MAX_SPACE-DPFIXED)/2)) {
            // cannot spare a record for its underflow sibling
            return false;
	  }
	  else {
            // move the first record to its sibling
	    
            // get the first record
            KeyDataEntry firstEntry;
            firstEntry=BT.getEntryFromBytes(getpage(), getSlotOffset(0),
					    getSlotLength(0), keyType,
					    NodeType.LEAF);
	    
            // insert it into its sibling
            MID dummyMid=new MID();
            leafPage.insertRecord(firstEntry);
            

            // delete the first record from the old page
            MID delMid=new MID();
            delMid.pageNo = getCurPage();
            delMid.slotNo = 0;
            if ( deleteSortedRecord(delMid) == false)
	      throw new LeafRedistributeException(null, "delete record failed");  
	    
	    
            // get the current first record of the old page
            // for adjusting parent pointer.
            KeyDataEntry tmpEntry;
            tmpEntry = getFirst(dummyMid);
         
            
            // adjust the entry pointing to itself in its parent
            st = parentIndexPage.adjustKey(tmpEntry.key, firstEntry.key);
            if( st==false) 
	      throw new LeafRedistributeException(null, "adjust key failed"); 
            return true;
	  }
	}
      }
      catch (Exception e) {
	throw new LeafRedistributeException(e, "redistribute failed");
      } 
    } // end of redistribute
  
} // end of BTLeafPage

    
 





















