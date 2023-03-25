package iterator;
import BigT.Map;
import heap.*;
import global.*;

import java.io.*;

/**
 *O_buf::Put takes maps and stores them on the buffer pages that
 *were passed to O_buf::init.  O_buf::flush inserts them enmass into
 *a temporary HeapFile.
 */
public class OBuf implements GlobalConst{
  
  /**
   *fault constructor
   * no args -- use init to initialize
   */
  public OBuf(){}     
  


  /**
   * O_buf is an output buffer. It takes as input:
   *@param bufs  temporary buffer to pages.(EACH ELEMENT IS A SINGLE BUFFER PAGE).
   *@param n_pages the number of pages
   *@param tSize   map size
   *@param temp_fd  fd of a  HeapFile
   *@param buffer  true => it is used as a buffer => if it is flushed, print
   *                      a nasty message. it is false by default.
  */
  public void init(byte[][] bufs, int n_pages, int tSize,
		   Heapfile temp_fd, boolean buffer )
    {
      _bufs    = bufs;
      _n_pages = n_pages;
      t_size   = tSize;
      _temp_fd = temp_fd;
      
      dirty       = false;
      t_per_pg    = MINIBASE_PAGESIZE / t_size;
      t_in_buf    = n_pages * t_per_pg;
      t_wr_to_pg  = 0;
      t_wr_to_buf = 0;
      t_written   = 0L;
      curr_page   = 0;
      buffer_only = buffer;
    }
  
  /**
   * Writes a map to the output buffer
   *@return the position of map which is in buffer
   *@exception IOException  some I/O fault
   *@exception Exception other exceptions
   * @param buf the map written to buffer
   */
  public Map Put(Map buf)
    throws IOException,
	   Exception
    {
      
      byte[] copybuf;
      copybuf = buf.getMapByteArray();
      System.arraycopy(copybuf,0,_bufs[curr_page],t_wr_to_pg*t_size, t_size);
      Map map_ptr = new Map(_bufs[curr_page] , t_wr_to_pg * t_size);
      //System.out.println("putting map: " + map_ptr.getRowLabel() + " vs: " + maptest.getRowLabel() + " with page size: " + t_per_pg);
      //System.out.println();
      t_written++; t_wr_to_pg++; t_wr_to_buf++; dirty = true;
      
      if (t_wr_to_buf == t_in_buf)                // Buffer full?
	{
	  flush();                                // Flush it
	  t_wr_to_pg = 0; t_wr_to_buf = 0;        // Initialize page info
	  curr_page  = 0;
	}
      else if (t_wr_to_pg == t_per_pg)
	{
	  t_wr_to_pg = 0;
	  curr_page++;
	}
      
      return map_ptr;
    }
  
  /**
   * returns the # of maps written.
   *@return the numbers of maps written
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public   long flush()  throws IOException, Exception
    {
      int count;
      int bytes_written = 0;
      byte[] tempbuf = new byte[t_size]; 
      if (buffer_only == true)
	System.out.println("Stupid error - but no error protocol");
      
      if (dirty)
	{
	  for (count = 0; count <= curr_page; count++)
	    {
	      MID mid;
	      // Will have to go thru entire buffer writing maps to disk
	      
	      if (count == curr_page)
		for (int i = 0; i < t_wr_to_pg; i++)
		  {
		    System.arraycopy(_bufs[count],t_size*i,tempbuf,0,t_size);
		    try {
		      mid =  _temp_fd.insertMap(tempbuf);
		    }
		    catch (Exception e){
		      throw e;
		    }
		  }
	      else
		for (int i = 0; i < t_per_pg; i++)
		  {       
		    System.arraycopy(_bufs[count],t_size*i,tempbuf,0,t_size);
		    try {
		      mid =  _temp_fd.insertMap(tempbuf);
		    }
		    catch (Exception e){
		      throw e;
		    }
		  }
	    }
	  
	  dirty = false;
	}
      
      return t_written;
    }
  
  private boolean dirty;                                // Does this buffer contain dirty pages?
  private  int  t_per_pg,                        // # of maps that fit in 1 page
    t_in_buf;                        // # of maps that fit in the buffer
  private  int  t_wr_to_pg,                        // # of maps written to current page
    t_wr_to_buf;                        // # of maps written to buffer.
  private  int  curr_page;                        // Current page being written to.
  private  byte[][]_bufs;                        // Array of pointers to buffer pages.
  private  int  _n_pages;                        // number of pages in array
  private  int  t_size;                                // Size of a map
  private  long t_written;                        // # of maps written so far.
  private  int  TEST_temp_fd;                        // fd of a temporary file
  private  Heapfile _temp_fd;
  private  boolean buffer_only;
}



