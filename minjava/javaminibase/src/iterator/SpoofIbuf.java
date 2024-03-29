package iterator;

import BigT.Map;
import heap.*;
import global.*;

import java.io.*;

public class SpoofIbuf implements GlobalConst  {
  
  /**
   *constructor, use the init to initialize
   */
  public void SpoofIbuf()
    {
      
      hf_scan = null;
    }
  
 
  /**
   *Initialize some necessary inormation, call Iobuf to create the
   *object, and call init to finish intantiation
   *@param bufs[][] the I/O buffer
   *@param n_pages the numbers of page of this buffer
   *@param tSize the map size
   *@param fd the reference to an Heapfile
   *@param Nmaps the map numbers of the page
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public  void init(Heapfile fd, byte bufs[][], int n_pages,
		    int tSize, int Nmaps)
    throws IOException,
	   Exception
    {
      _fd       = fd;       _bufs        = bufs;
      _n_pages  = n_pages;  t_size       = tSize;
      
      t_proc    = 0;        t_in_buf     = 0;
      tot_t_proc= 0;
      curr_page = 0;        t_rd_from_pg = 0;
      done      = false;    t_per_pg     = MINIBASE_PAGESIZE / t_size;
     
      
      n_maps = Nmaps;
     
      // open a scan
      if (hf_scan != null)  hf_scan = null;
      
      try {
	hf_scan = _fd.openScan();
      }
      catch(Exception e){
	throw e;
      }
      
      
    }
  
   /** 
   *get a map from current buffer,pass reference buf to this method
   *usage:temp_map = map.Get(buf); 
   *@param buf write the result to buf
   *@return the result map
   *@exception IOException some I/O fault
   *@exception Exception other exceptions
   */
  public Map Get(Map  buf)throws IOException, Exception
    {
      if (tot_t_proc == n_maps) done = true;
      
      if (done == true){buf = null; return null;}
      if (t_proc == t_in_buf)
	{
	  try {
	    t_in_buf = readin();
	  }
	  catch (Exception e){
	    throw e;
	  }
	  curr_page = 0; t_rd_from_pg = 0; t_proc = 0;
	}
      
      if (t_in_buf == 0)                        // No maps read in?
	{
	  done = true; buf = null;return null;
	}
 
      buf.mapSet(_bufs[curr_page],t_rd_from_pg*t_size);
        //System.out.println("getting map: " + buf.getRowLabel() + "\n");//+ " vs: " + maptest.getRowLabel() + " with page size: " + t_per_pg);


      tot_t_proc++;
      
      // Setup for next read
      t_rd_from_pg++; t_proc++;
      if (t_rd_from_pg == t_per_pg)
	{
	  t_rd_from_pg = 0; curr_page++;
	}
      return buf;
    }
  
   
  /**
   *@return if the buffer is empty,return true. otherwise false
   */
  public  boolean empty()
    {
      if (tot_t_proc == n_maps) done = true;
      return done;
    }
  
  /**
   *
   *@return the numbers of maps in the buffer
   *@exception IOException some I/O fault
   *@exception InvalidMapSizeException Heapfile error
   */
  private int readin()throws IOException, InvalidMapSizeException
    {
      int   t_read = 0, tot_read = 0;
      Map t      = new Map ();
      byte[] t_copy;
      
      curr_page = 0;
      while (curr_page < _n_pages)
	{
	  while (t_read < t_per_pg)
	    {
	      MID mid =new MID();
	      try {
		if ( (t = hf_scan.getNext(mid)) == null) return tot_read;
		t_copy = t.getMapByteArray();
		System.arraycopy(t_copy,0,_bufs[curr_page],t_read*t_size,t_size); 
	      }
	      catch (Exception e) {
		System.err.println (""+e);
	      }
	      t_read++; tot_read++;
	    } 
	  t_read     = 0;
	  curr_page++;
	}
      return tot_read;
    }
  
  
  private  byte[][] _bufs;
  
  private  int   TEST_fd;
  
  private  Heapfile _fd;
  private  Scan hf_scan;
  private  int    _n_pages;
  private  int    t_size;
  
  private  int    t_proc, t_in_buf;
  private  int    tot_t_proc;
  private  int    t_rd_from_pg, curr_page;
  private  int    t_per_pg;
  private  boolean   done;
  private  int    n_maps;
public void close() {
}
}


