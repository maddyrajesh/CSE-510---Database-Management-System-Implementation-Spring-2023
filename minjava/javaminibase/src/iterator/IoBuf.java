
package iterator;
import BigT.Map;
import heap.*;
import global.*;

import java.io.*;

public class IoBuf implements GlobalConst{
  /**
   * Constructor - use init to initialize.
   */
  public void IoBuf(){}             
  
  /**
   *Initialize some necessary inormation, call Iobuf to create the
   *object, and call init to finish instantiation
   *@param bufs[][] the I/O buffer
   *@param n_pages the numbers of page of this buffer
   *@param tSize the page size
   *@param temp_fd the reference to a Heapfile
   */ 
  public void init(byte bufs[][], int n_pages, int tSize, Heapfile temp_fd)
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
      flushed     = false;
      mode        = WRITE_BUFFER;
      i_buf       = new SpoofIbuf();
      done        = false;
    }
  
  
  /**
   * Writes a map to the output buffer
   *@exception NoOutputBuffer the buffer is a input bufer now
   *@exception IOException  some I/O fault
   *@exception Exception  other exceptions
   * @param buf the map written to buffer
   */
  public void Put(Map buf)
    throws NoOutputBuffer,
	   IOException,
	   Exception
    {
      if (mode != WRITE_BUFFER)
	throw new NoOutputBuffer("IoBuf:Trying to write to io buffer when it is acting as a input buffer");
      
      byte[] copybuf;
      copybuf = buf.getMapByteArray();
      System.arraycopy(copybuf,0,_bufs[curr_page],t_wr_to_pg*t_size,t_size); 
      
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
      return;
    }

  /**
   *get a map from current buffer,pass reference buf to this method
   *usage:temp_map = map.Get(buf);
   *@return the result map
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   * @param buf write the result to buf
   */
  public Map Get(Map buf)
    throws IOException,
	   Exception
    {
      Map tempmap;
      if (done){
	buf =null;
	return null;
      }
      if (mode == WRITE_BUFFER)     // Switching from writing to reading?
	reread();
      
      if (flushed)
	{
	  // get maps from
	  if ((tempmap= i_buf.Get(buf)) == null)
	    {
	      done = true;
	      return null;
	    }
	}
      else
	{
	  // just reading maps from the buffer pages.
	  if ((curr_page * t_per_pg + t_rd_from_pg) == t_written)
	    {
	      done = true;
	      buf = null;
	      return null;
	    }
	  buf.mapSet(_bufs[curr_page],t_rd_from_pg*t_size);
	  
	  // Setup for next read
	  t_rd_from_pg++;
	  if (t_rd_from_pg == t_per_pg)
	    {
	      t_rd_from_pg = 0; curr_page++;
	    }
	}
      
      return buf;
    }
  
  
  /**
   * returns the numbers of maps written
   *@return the numbers of maps written
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public long flush()throws IOException, Exception 
    {
      int count;
      byte [] tempbuf = new byte [t_size];
      
      flushed = true;
      if (dirty)
	{
	  for (count = 0; count <= curr_page; count++)
	    {
	      MID mid;

	      // Will have to go thru entire buffer writing maps to disk
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
	    }
	  dirty = false;
	}
      
      return t_written;
    }
  
  /**
   *if WRITE_BUFFER is true, call this mehtod to switch to read buffer.
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public void reread()
    throws IOException,
	   Exception
    {
      
      mode = READ_BUFFER;
      if (flushed)                   // Has the output buffe been flushed?
	{
	  // flush all the remaining maps to disk.
	  flush();
	  i_buf.init(_temp_fd, _bufs, _n_pages, t_size, (int)t_written);
	}
      else
	{
	  // All the maps are in the buffer, just read them out.
	  t_rd_from_pg = 0;
	  curr_page    = 0;
	}
    }   
  
  public static final int WRITE_BUFFER =0;
  public static final int READ_BUFFER  =1;
  private boolean done;
  private  boolean dirty;              // Does this buffer contain dirty pages?
  private  int  t_per_pg,              // # of maps that fit in 1 page
    t_in_buf;                        // # of maps that fit in the buffer
  private  int  t_wr_to_pg,          // # of maps written to current page
    t_wr_to_buf;                      // # of maps written to buffer.
  private  int  curr_page;            // Current page being written to.
  private  byte _bufs[][];            // Array of pointers to buffer pages.
  private  int  _n_pages;             // number of pages in array
  private  int  t_size;               // Size of a map
  private  long t_written;           // # of maps written so far
  private  int  _TEST_temp_fd;       // fd of a temporary file
  private  Heapfile _temp_fd;
  private  boolean  flushed;        // TRUE => buffer has been flushed.
  private  int  mode;
  private  int  t_rd_from_pg;      // # of maps read from current page
  private  SpoofIbuf i_buf;        // gets input from a temporary file
}








