package simpledb.buffer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import simpledb.file.Block;
import simpledb.file.FileMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
	/*
	 * Changing bufferpool array to HashMap for faster access
	 * @author Akshay Jain
	 */
   private HashMap<Block, Buffer> bufferpool;
   private int numAvailable;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
	   /*
	    * Changing initialization according to hashmap change for faster access
	    * @author Akshay Jain
	    */
      bufferpool = new HashMap<Block, Buffer>(numbuffs);
      numAvailable = numbuffs;
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
	   /*
	    * Iterating over bufferpool (hashmap) values to flush all buffers
	    * @author Akshay Jain
	    */
	   try{
		   Iterator<Buffer> it = bufferpool.values().iterator();
			  while(it.hasNext()){
				  Buffer buff = it.next();
		          if (buff.isModifiedBy(txnum))
		        	  buff.flush();
			  }
	   }
	  catch(Exception ex){
    	  System.out.println(ex.getMessage());
      }
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         /*
          * Adding new block with its buffer to hash map
          * @author Akshay Jain
          */
         bufferpool.put(blk, buff);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      Block blk = buff.assignToNew(filename, fmtr);
      /*
       * Adding new block with its buffer to hash map
       * @author Akshay Jain
       */
      bufferpool.put(blk, buff);
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   /*
   * Find existing buffer based on key
   * @param blk Block to find in bufferpool
   * @return the buffer mapped to the block
   * @author Akshay Jain
   */
   private Buffer findExistingBuffer(Block blk) {
	  
      if(bufferpool.containsKey(blk))
    	  return bufferpool.get(blk);
      else
    	  return null;
   }
   
   /*
    * Choose unpinned buffer to be replaced according to LRU2 policy
    * @return unpinned buffer appropriate for replacement
    * @author Akshay Jain
    */
   private Buffer chooseUnpinnedBuffer() {
	  if(bufferpool.size() < numAvailable)
		  return new Buffer();
	  
	  Iterator<Buffer> it = bufferpool.values().iterator();
	  List<Buffer> unpinnedBuffers = new ArrayList<Buffer>();
	  while(it.hasNext()){
		 Buffer buff = it.next(); 
		 if (!buff.isPinned()){
			 unpinnedBuffers.add(buff);
		 }
	  }
	  
	  /*
	   * If no unpinned buffer, return null
	   * @author Akshay Jain
	   */
	  if(unpinnedBuffers.size() <= 0)
		  return null;
	  else{
		  Buffer buff = getLRU2(unpinnedBuffers);
		  //Removing from bufferpool since this will be replaced and holds no value anymore
		  bufferpool.values().remove(buff);
		  return buff;
	  }
   }
   
   /*
    * LRU implementation according to buffers' access times
    * @param unpinnedBuffers unpinned buffers currently in the pool
    * @return Buffer -unpinned buffer according to LRU2 policy
    */
   private Buffer getLRU2(List<Buffer> unpinnedBuffers){
	   long minDate = Long.MAX_VALUE;
	   
	   //Getting minimum second last date
	   for(Buffer buff: unpinnedBuffers){
		   if(buff.getSecondLastAccess() <= minDate)
			   minDate = buff.getSecondLastAccess();
	   }
	   
	   //Collecting all buffers with secondLastAccess as minDate
	   List<Buffer> lruBuffers = new ArrayList<Buffer>();
	   for(Buffer buff: unpinnedBuffers){
		   if(buff.getSecondLastAccess() == minDate)
			   lruBuffers.add(buff);
	   }
	   
	   //Returning buffer if only 1 buffer exists with minimum secondLastAccess
	   if(lruBuffers.size() == 1)
		   return lruBuffers.get(0);
	   
	   //Otherwise, falling to traditional LRU for these buffers
	   Buffer minBuffer = lruBuffers.get(0);
	   minDate = minBuffer.getLastAccess();
	   lruBuffers.remove(0);
	   for(Buffer buff: lruBuffers){
		   if(buff.getLastAccess() <= minDate)
			   minBuffer = buff;
	   }
	   return minBuffer;
   }
}
