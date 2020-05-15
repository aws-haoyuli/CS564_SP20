/**
 * Purpose: Implementation of Buffer Manager
 *
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 * @author Jiawei Gu, 9076085472
 * @author Haoyu Li, 9081478068
 * @author Xiaoqi Li, 9077696772
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include <iostream>
#include <memory>
#include "buffer.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb
{

BufMgr::BufMgr(std::uint32_t bufs) : numBufs(bufs)
{
  bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++)
  {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
  hashTable = new BufHashTbl(htsize); // allocate the buffer hash table
  bufStats.clear();

  clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{
  for (std::uint32_t i = 0; i < numBufs; i++)
  {
    BufDesc currDesc = bufDescTable[i];
    if (currDesc.dirty && File::isOpen(currDesc.file->filename()))
    {
      Page dirtyPage = bufPool[i];
      currDesc.file->writePage(dirtyPage);
      currDesc.dirty = false;
      bufStats.diskwrites++;
    }
  }
  delete[] bufDescTable;
  delete[] bufPool;
  bufDescTable = NULL;
  bufPool = NULL;
}

void BufMgr::advanceClock()
{
  clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId &frame)
{
  uint32_t pinNum = 0;
  bool lastPin = false;
  // at most two circle, assume the first circle set all refbit to false
  // if after two circles there is still no pages, no need to find
  for (uint32_t i = 0; i <= 2 * numBufs; i++)
  {
    // check next location
    advanceClock();
    // std::cout<<"i = "<<i<<"\tclockHand = "<<clockHand<<"\tpin_num = "<<pin_num<<std::endl;
    // all buffer frames are pinned
    if (pinNum == numBufs)
    {
      throw BufferExceededException();
    }
    // invalid, we can use it, outer function will set it
    if (!bufDescTable[clockHand].valid)
    {
      frame = clockHand;
      bufStats.accesses++;
      return;
    }
    // handle refbit = true
    if (bufDescTable[clockHand].refbit)
    {
      bufDescTable[clockHand].refbit = false;
      continue;
    }
    // if there are continuos numBufs pinned pages
    // we can see all buffer frames are pinned
    if (bufDescTable[clockHand].pinCnt > 0)
    {
      if (lastPin)
      {
        pinNum++;
      }
      else
      {
        lastPin = true;
        pinNum = 1;
      }
      continue;
    }
    else
    {
      lastPin = false;
    }
    if (bufDescTable[clockHand].dirty)
    {
      // writing a dirty page back to disk.
      bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
      bufDescTable[clockHand].dirty = false;
      bufStats.diskwrites++;
    }
    // the buffer frame allocated has a valid page in it, you remove the
    // appropriate entry from the hash table.
    hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
    bufDescTable[clockHand].Clear();
    frame = clockHand;
    bufStats.accesses++;
    return;
  }
}

void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
{
  FrameId frameNo = -1;
  try
  {
    // First checkwhether the page is already in the buffer pool by invoking
    // the lookup()method
    hashTable->lookup(file, pageNo, frameNo);
    // Page is in the buffer pool
    // set the appropriate refbit
    bufDescTable[frameNo].refbit = true;
    // increment the pinCnt for the page
    bufDescTable[frameNo].pinCnt++;
    // return a pointer to the frame containing the page
    // via the page parameter.
    page = &bufPool[frameNo];
  }
  catch (const HashNotFoundException &e)
  {
    // Page is not in the buffer pool
    // Call allocBuf() to allocate a buffer frame
    allocBuf(frameNo);
    // call the method file->readPage() to read the page from disk
    // into the buffer pool frame.
    Page pageTmp = file->readPage(pageNo);
    bufPool[frameNo] = pageTmp;
    // insert the page into the hashtable
    hashTable->insert(file, pageNo, frameNo);
    // invoke Set() on the frame to set it up properly
    bufDescTable[frameNo].Set(file, pageNo);
    // Return a pointer to the frame containing the page via the page
    // parameter.
    page = &bufPool[frameNo];
    bufStats.diskreads++;
  }
  bufStats.accesses++;
}

void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
  FrameId frameNum;
  try
  {
    // look up frame with matching file and pageId
    hashTable->lookup(file, pageNo, frameNum);
    // throw Page Not Pinned Exception if pincount <= 0
    if (bufDescTable[frameNum].pinCnt <= 0)
    {
      throw PageNotPinnedException(file->filename(), pageNo, frameNum);
    }
    // decrement pincount otherwise
    bufDescTable[frameNum].pinCnt--;
    // set up dirty bit if true
    if (dirty)
    {
      bufDescTable[frameNum].dirty = true;
    }
    bufStats.accesses++;
  }
  catch (const HashNotFoundException &e)
  {
    // do nothing
  }
}

void BufMgr::flushFile(const File *file)
{
  for (std::uint32_t i = 0; i < numBufs; i++)
  {
    BufDesc *currDesc = &bufDescTable[i];
    bufStats.accesses++;
    if (file == currDesc->file)
    {
      // perform error checking
      if (!currDesc->valid)
      {
        throw BadBufferException(currDesc->frameNo, currDesc->dirty, currDesc->valid, currDesc->refbit);
        break;
      }
      if (currDesc->pinCnt > 0)
      {
        throw PagePinnedException(file->filename(), currDesc->pageNo, currDesc->frameNo);
        break;
      }

      // if dirty, write to file
      if (currDesc->dirty)
      {
        Page dirtyPage = bufPool[currDesc->frameNo];
        currDesc->file->writePage(dirtyPage);
        currDesc->dirty = false;
        bufStats.diskwrites++;
      }

      // remove from hashTable and clear the desc
      hashTable->remove(file, currDesc->pageNo);
      currDesc->Clear();
    }
    else
    {
      continue;
    }
  }
}

void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
  // std::cout<<"allocPage 1 begin!"<<std::endl;
  // alloc a new and empty page in file
  Page temp = file->allocatePage();
  pageNo = temp.page_number();
  // obtain an available buffer pool frame
  FrameId frameNum;
  allocBuf(frameNum);
  bufPool[frameNum] = temp;
  // insert the entry into hashtable
  hashTable->insert(file, pageNo, frameNum);
  // set up the frame properly
  bufDescTable[frameNum].Set(file, pageNo);
  page = &bufPool[frameNum];
  bufStats.accesses++;
  // std::cout<<"allocPage 1 end!"<<std::endl;
}

void BufMgr::disposePage(File *file, const PageId PageNo)
{
  try
  {
    // find the frameId corresponding to the file and pageNo
    FrameId frameId = 0;
    hashTable->lookup(file, PageNo, frameId);

    // remove the page from hashTable and clear the bufDesc
    hashTable->remove(file, PageNo);
    bufDescTable[frameId].Clear();
  }
  catch (const HashNotFoundException &)
  {
    // page not in hashTable, do nothing
  }
  // delete page from file
  file->deletePage(PageNo);
}

void BufMgr::printSelf(void)
{
  BufDesc *tmpbuf;
  int validFrames = 0;

  for (std::uint32_t i = 0; i < numBufs; i++)
  {
    tmpbuf = &(bufDescTable[i]);
    std::cout << "FrameNo:" << i << " ";
    tmpbuf->Print();

    if (tmpbuf->valid == true)
      validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

} // namespace badgerdb
