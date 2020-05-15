/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "btree.h"

#include <assert.h>

#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"

//#define DEBUG

namespace badgerdb {

void page_set(IndexMetaInfo *newNode){
    memset(newNode->relationName, 0, 20);
    newNode->attrByteOffset = 0;
    newNode->attrType = INTEGER;
    newNode->rootPageNo = 0;
    newNode->initialRootPageNum = 0;
}

void page_set(NonLeafNodeInt *newNode){
    newNode->level = 0;
    newNode->keyNum = 0;
    for (int i = 0 ; i < INTARRAYNONLEAFSIZE ; i ++){
        newNode->keyArray[i] = 0;
        newNode->pageNoArray[i] = 0;
    }
    newNode->pageNoArray[INTARRAYNONLEAFSIZE] = 0;
}

void page_set(LeafNodeInt *newNode){
    newNode->rightSibPageNo = 0;
    newNode->keyNum = 0;
    for (int i = 0 ; i < INTARRAYLEAFSIZE ; i ++){
        newNode->keyArray[i] = 0;
        newNode->ridArray[i].page_number = 0;
        newNode->ridArray[i].slot_number = 0;
    }
}

/**
 * Allocate a NonLeafNodeInt or LeafNodeInt
 *
 * @param newPageId page id used to contain allocated page id
 * @return newNode NonLeafNodeInt or LeafNodeInt
 */
template <class T>
T *BTreeIndex::allocNode(PageId &newPageId) {
    Page *dummy;
    bufMgr->allocPage(file, newPageId, dummy);
    T *newNode = (T *)dummy;
    page_set(newNode);
    return newNode;
}

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

/**
 * Constructor
 *
 * The constructor first checks if the specified index file exists.
 * If the index file exists, the file is opened. Else, a new index file is
 * created.
 *
 * @param relationName name of the relation on which to build the index
 * @param outIndexName name of the index file
 * @param bufMgrIn global buffer manager
 * @param attrByteOffset byte offset of the attribute to build the index
 * @param attrType data type of the indexing attribute
 */
BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName, BufMgr *bufMgrIn,
                       const int attrByteOffset, const Datatype attrType) {
  // initialize global varaibles
  this->bufMgr = bufMgrIn;
  this->attrByteOffset = attrByteOffset;
  this->attributeType = attrType;

  // construct index name
  std::ostringstream idxstr;
  idxstr << relationName << '.' << attrByteOffset;
  outIndexName = idxstr.str();

  // test if index file exists
  try {
    this->file = new BlobFile(outIndexName, false);
    // read meta info
    this->headerPageNum = file->getFirstPageNo();
    Page *headerPage;
    bufMgr->readPage(file, headerPageNum, headerPage);
    // get the page that contain meta info
    IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;

    if (relationName != meta->relationName || attrType != meta->attrType ||
        attrByteOffset != meta->attrByteOffset) {
      throw BadIndexInfoException(outIndexName);
    }

    // set B-tree object
    this->rootPageNum = meta->rootPageNo;
    this->initialRootPageNum = meta->initialRootPageNum;
    this->scanExecuting = false;

    // write page
    bufMgr->unPinPage(file, headerPageNum, false);
  } catch (BadgerDbException &e) {
    file = new BlobFile(outIndexName, true);
    IndexMetaInfo *meta = allocNode<IndexMetaInfo>(this->headerPageNum);

    // initialize B+ tree object in b-tree the init root page should be leaf
    LeafNodeInt *page = allocNode<LeafNodeInt>(this->rootPageNum);
    this->initialRootPageNum = this->rootPageNum;
    this->scanExecuting = false;
    page->rightSibPageNo = 0;

    // initialize indexMetaInfo for header page
    meta->rootPageNo = this->rootPageNum;
    meta->initialRootPageNum = this->initialRootPageNum;
    meta->attrByteOffset = attrByteOffset;
    meta->attrType = attrType;
    strncpy(meta->relationName, relationName.c_str(), 20);

    // set some meta info for B+ tree

    // write page
    bufMgr->unPinPage(file, this->rootPageNum, true);
    bufMgr->unPinPage(file, this->headerPageNum, true);

    // scan the relation and insert entries
    FileScan scan(relationName, bufMgr);
    RecordId rid;
    try {
      while (true) {
        scan.scanNext(rid);
        std::string recordStr = scan.getRecord();
        const char *record = recordStr.c_str();
        int key = *((int *)(record + attrByteOffset));
        insertEntry(&key, rid);
      }
    } catch (EndOfFileException &e) {
    }
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
/**
 * Destructor
 *
 * clearing up any state variables,
 * unpinning any B+ Tree pages that are pinned,
 * and flushing the index file
 *
 * Note that this method does not delete the index file!
 * But, deletion of the file object is required,
 * which will call thedestructor of File class causing the index file to be
 * closed.
 */
BTreeIndex::~BTreeIndex() {
  if (scanExecuting) {
    endScan();
  }
  bufMgr->flushFile(this->file);
  delete this->file;
  this->file = nullptr;
}

/**
 * @param  page given NonLeafNode
 * @return whether it is a non leaf node just above the leaf node
 */
bool BTreeIndex::isLevelOneNode(const NonLeafNodeInt *page) {
  return page->level == 1;
}

/**
 * findIndexInNonLeaf
 *
 * @param nonLeafNode
 * @param key
 * @return loc
 */
int BTreeIndex::findIndexInNonLeaf(const NonLeafNodeInt *nonLeafNode,
                                   const void *key) {
  int loc = std::lower_bound(nonLeafNode->keyArray,
                             nonLeafNode->keyArray + nonLeafNode->keyNum,
                             *(int *)key) -
            nonLeafNode->keyArray;
  // in case page is full and then seg fault
  return loc > INTARRAYNONLEAFSIZE ? -1 : loc;
}

/**
 * Given a key, find index of the first element that larger than it
 * For 1 3 5 7 and key = 2 return 1
 * If array is empty, will return 0
 *
 * @param leafNode
 * @param key
 * @return loc
 */
int BTreeIndex::findIndexInLeaf(const LeafNodeInt *leafNode, const void *key) {
  int loc =
      std::lower_bound(leafNode->keyArray,
                       leafNode->keyArray + leafNode->keyNum, *(int *)key) -
      leafNode->keyArray;
  return loc > INTARRAYLEAFSIZE ? -1 : loc;
}

/**
 * insertToLeaf
 *
 * @param leafNode
 * @param index
 * @param key
 * @param rid
 */
void BTreeIndex::insertToLeaf(LeafNodeInt *leafNode, const int index,
                              const void *key, const RecordId rid) {
  const size_t len = INTARRAYLEAFSIZE - index - 1;

  // shift elements from the index
  memmove(&leafNode->keyArray[index + 1], &leafNode->keyArray[index],
          len * sizeof(int));
  memmove(&leafNode->ridArray[index + 1], &leafNode->ridArray[index],
          len * sizeof(RecordId));

  leafNode->keyNum++;
  this->leafOccupancy++;

  // store the KV into this node
  leafNode->keyArray[index] = *(int *)key;
  leafNode->ridArray[index] = rid;
}

/**
 * When insert into
 *     1     |     3    |   5
 *    /      |          |    \
 *  (-,1)   [1, 3)    [3,5)  [5,+)
 * @param currNonLeafNode
 * @param index
 * @param newIndex
 * @param newPageNo
 */
void BTreeIndex::insertToNonLeaf(NonLeafNodeInt *currNonLeafNode,
                                 const int index, const int newIndex,
                                 const PageId newPageNo) {
  const size_t len = INTARRAYNONLEAFSIZE - index - 1;

  // shift elements from the index
  memmove(&currNonLeafNode->keyArray[index + 1],
          &currNonLeafNode->keyArray[index], len * sizeof(int));
  memmove(&currNonLeafNode->pageNoArray[index + 2],
          &currNonLeafNode->pageNoArray[index + 1], len * sizeof(PageId));

  currNonLeafNode->keyNum++;
  this->nodeOccupancy++;

  // store the KV into this node
  currNonLeafNode->keyArray[index] = newIndex;
  currNonLeafNode->pageNoArray[index + 1] = newPageNo;
}

/**
 * Because after split a non leaf node, the new node is like
 *     7     |     9    |   11
 *           |          |    \
 *          [7, 9)    [9,11)  [11,+)
 * So it's a little different from above insertToNonLeaf()
 * @param currNonLeafNode
 * @param index
 * @param newIndex
 * @param newPageNo
 */
void BTreeIndex::insertToNewNonLeaf(NonLeafNodeInt *currNonLeafNode,
                                    const int index, const int newIndex,
                                    const PageId newPageNo) {
  const size_t len = INTARRAYNONLEAFSIZE - index - 1;

  // shift elements from the index
  memmove(&currNonLeafNode->keyArray[index + 1],
          &currNonLeafNode->keyArray[index], len * sizeof(int));
  memmove(&currNonLeafNode->pageNoArray[index + 1],
          &currNonLeafNode->pageNoArray[index], len * sizeof(PageId));

  this->nodeOccupancy++;
  currNonLeafNode->keyNum++;

  // store the KV into this node
  currNonLeafNode->keyArray[index] = newIndex;
  currNonLeafNode->pageNoArray[index] = newPageNo;
}

/**
 * for
 *     7     |     9    |   11
 *           |          |    \
 *          [7, 9)    [9,11)  [11,+)
 * convert to
 *                 9    |   11
 *            /         |    \
 *          [7, 9)    [9,11)  [11,+)
 * @param currNonLeafNode
 * @return the smallest key(the parent needs)
 */
int BTreeIndex::deleteNewKeyNonLeaf(NonLeafNodeInt *currNonLeafNode) {
  currNonLeafNode->keyNum--;
  this->nodeOccupancy--;
  int key = currNonLeafNode->keyArray[0];
  memmove(&currNonLeafNode->keyArray[0], &currNonLeafNode->keyArray[1],
          (currNonLeafNode->keyNum) * sizeof(int));
  currNonLeafNode->keyArray[currNonLeafNode->keyNum] = 0;
  return key;
}

/**
 * Split a leaf node into 2 parts
 *
 * @param node
 * @param newNode
 * @param leftLen
 */
void BTreeIndex::splitLeaf(LeafNodeInt *node, LeafNodeInt *newNode,
                           PageId newPageNo, const int leftLen) {
  const size_t rightLen = INTARRAYLEAFSIZE - leftLen;

  // adjust keyNum
  node->keyNum = leftLen;
  newNode->keyNum = rightLen;

  // move elements from old node to new node
  memcpy(newNode->keyArray, &node->keyArray[leftLen], rightLen * sizeof(int));
  memcpy(newNode->ridArray, &node->ridArray[leftLen],
         rightLen * sizeof(RecordId));

  // clear the space of removed elements
  memset(&node->keyArray[leftLen], 0, rightLen * sizeof(int));
  memset(&node->ridArray[leftLen], 0, rightLen * sizeof(RecordId));

  // set rightSibPageNo
  newNode->rightSibPageNo = node->rightSibPageNo;
  node->rightSibPageNo = newPageNo;
}

/**
 * Split a non leaf node into 2 parts
 * from
 *     1     |     3    |     5    |   7   |   9    |   11
 *    /      |          |          |       |        |     \
 *  (-,1)   [1, 3)    [3,5)       [5,7)   [7,9)    [9,11)  [11,+)
 *  to
 *     1     |     3    |   5
 *    /      |          |    \
 *  (-,1)   [1, 3)    [3,5)  [5,+)
 * and
 *     7     |     9    |   11
 *           |          |    \
 *          [7, 9)    [9,11)  [11,+)
 * @param node
 * @param newNode
 * @param leftLen
 */
void BTreeIndex::splitNonLeaf(NonLeafNodeInt *node, NonLeafNodeInt *newNode,
                              const int leftLen) {
  const size_t rightLen = INTARRAYNONLEAFSIZE - leftLen;

  // adjust keyNum
  node->keyNum = leftLen;
  newNode->keyNum = rightLen;

  // move elements from old node to new node
  memcpy(newNode->keyArray, &node->keyArray[leftLen], rightLen * sizeof(int));
  memcpy(newNode->pageNoArray, &node->pageNoArray[leftLen + 1],
         rightLen * sizeof(PageId));

  newNode->level = (node->level == 1);

  // clear the space of removed elements
  memset(&node->keyArray[leftLen], 0, rightLen * sizeof(int));
  memset(&node->pageNoArray[leftLen + 1], 0, rightLen * sizeof(PageId));

  // printNode(node);
  // printNode(newNode);
}

/**
 * When need to split the root, alloc a new page as new root and finish
 * related settings
 *
 * @param key    new parent key
 * @param left   smaller one
 * @param right	 larger one
 */
void BTreeIndex::splitRoot(const int key, const PageId left,
                           const PageId right) {
  // alloc a new page for root
  PageId newRootPageId;
  NonLeafNodeInt *newRoot = allocNode<NonLeafNodeInt>(newRootPageId);

  // set newRoot
  newRoot->keyArray[0] = key;
  newRoot->pageNoArray[0] = left;
  newRoot->pageNoArray[1] = right;
  newRoot->keyNum = 1;
  this->nodeOccupancy++;

  // set level
  newRoot->level = (this->rootPageNum == this->initialRootPageNum);

  // unpin the root page
  bufMgr->unPinPage(file, newRootPageId, true);
  this->rootPageNum = newRootPageId;

  // address change of root page
  Page *page;
  bufMgr->readPage(file, this->headerPageNum, page);
  IndexMetaInfo *meta = (IndexMetaInfo *)page;
  meta->rootPageNo = newRootPageId;
  bufMgr->unPinPage(file, this->headerPageNum, true);
}

/**
 * When we can make sure we want to insert a KV to a leaf node
 *
 * @param currPageNo
 * @param key
 * @param rid
 * @param newPageNo
 * @param newIndex
 */
void BTreeIndex::handleLeafInsertion(const PageId currPageNo, const void *key,
                                     const RecordId rid, PageId &newPageNo,
                                     int &newIndex) {
  Page *page;
  bufMgr->readPage(file, currPageNo, page);
  LeafNodeInt *currLeafNode = (LeafNodeInt *)page;

  // We are sure it is a LeafNode
  int index = findIndexInLeaf(currLeafNode, key);
  // not full, insert directly
  if (currLeafNode->keyNum < INTARRAYLEAFSIZE) {
    assert(index != -1);
    insertToLeaf(currLeafNode, index, key, rid);
    bufMgr->unPinPage(file, currPageNo, true);
    newPageNo = 0;
    newIndex = -1;
    return;
  }

  // allocate a new LeafNode page
  LeafNodeInt *newNode = allocNode<LeafNodeInt>(newPageNo);
  // full, prepare to split this leaf node
  const int middle = INTARRAYLEAFSIZE / 2;
  bool insertToLeft = index < middle;

  // if full size = 7 and insert index < 3, then split into 3 and 4
  // if full size = 7 and insert index >= 3, then split into 4 and 3
  // if full size = 8 and insert index < 4, then split into 4 and 4
  // if full size = 8 and insert index >= 4, then split into 5 and 3
  splitLeaf(currLeafNode, newNode, newPageNo, middle + (!insertToLeft));

  // insert the key and record id to the node
  if (insertToLeft) {
    insertToLeaf(currLeafNode, index, key, rid);
  } else {
    insertToLeaf(newNode, index - (middle + (!insertToLeft)), key, rid);
  }

  assert(currLeafNode->keyNum - newNode->keyNum <= 1 &&
         currLeafNode->keyNum - newNode->keyNum >= -1);

  // set the return value
  newIndex = newNode->keyArray[0];

  // unpin the new node and the original node
  bufMgr->unPinPage(file, currPageNo, true);
  bufMgr->unPinPage(file, newPageNo, true);
}
/**
 * recursive call until leaf nodes
 *
 * @param currPageNo page id of the page that stores the root node of the
 *        subtree.
 * @param key the key of the key-record pair to be inserted
 * @param rid the record ID of the key-record pair to be inserted
 * @param newPageNo a pointer to an integer value to be stored in the parent
 * node if the insertion requires a split in the current level
 * @param isLeaf whether this node is leaf
 * @param newIndex the page number of the newly created node if a split occurs,
 * or 0 otherwise.
 */
void BTreeIndex::recursiveInsert(const PageId currPageNo, const void *key,
                                 const RecordId rid, PageId &newPageNo,
                                 int &newIndex, bool isLeaf) {
  // if leaf, just call handleLeafInsertion
  if (isLeaf) {
    handleLeafInsertion(currPageNo, key, rid, newPageNo, newIndex);
    return;
  }

  // get the current page, it must be a non leaf node
  Page *page;
  bufMgr->readPage(file, currPageNo, page);
  NonLeafNodeInt *currNonLeafNode = (NonLeafNodeInt *)page;

  // find the node to be inserted and recursive call
  int childIndex = findIndexInNonLeaf(currNonLeafNode, key);
  PageId nodeBeInserted = currNonLeafNode->pageNoArray[childIndex];
  recursiveInsert(nodeBeInserted, key, rid, newPageNo, newIndex,
                  isLevelOneNode(currNonLeafNode));

  // no split in child
  if (newPageNo == 0) {
    bufMgr->unPinPage(file, currPageNo, true);
    newIndex = -1;
    return;
  }

  // split happened in child, need to insert new key and corresponding pageNO
  // into current page not full just return
  int index = findIndexInNonLeaf(currNonLeafNode, (void *)(&newIndex));
  if (currNonLeafNode->keyNum < INTARRAYNONLEAFSIZE) {
    assert(index != -1);
    insertToNonLeaf(currNonLeafNode, index, newIndex, newPageNo);
    bufMgr->unPinPage(file, currPageNo, true);
    newPageNo = 0;
    newIndex = -1;
    return;
  }

  // this page is also full, allocate a new NonLeafNode
  PageId newNonLeafPage;
  NonLeafNodeInt *newNode = allocNode<NonLeafNodeInt>(newNonLeafPage);

  // need split this page, tell parent through
  // newPageNo & newIndex the middle index for spliting the page
  const int middle = INTARRAYNONLEAFSIZE / 2;
  bool insertToLeft = index < middle;

  // split the node to currNonLeafNode and newNode
  splitNonLeaf(currNonLeafNode, newNode, middle + (!insertToLeft));

  // insert key and record id to the right node
  if (insertToLeft) {
    insertToNonLeaf(currNonLeafNode, index, newIndex, newPageNo);
  } else {
    insertToNewNonLeaf(newNode, index - (middle + (!insertToLeft)), newIndex,
                       newPageNo);
  }
  // printNode(newNode);

  assert(currNonLeafNode->keyNum - newNode->keyNum <= 1 &&
         currNonLeafNode->keyNum - newNode->keyNum >= -1);

  // new node is illeagle now, so adjust it
  newIndex = deleteNewKeyNonLeaf(newNode);
  newPageNo = newNonLeafPage;

  // unpin the new node and the currPageNo node
  bufMgr->unPinPage(file, currPageNo, true);
  bufMgr->unPinPage(file, newPageNo, true);
}

/**
 * Insert a new entry using the pair <value,rid>.
 * Start from root to recursively find out the leaf to insert the entry in. The
 *insertion may cause splitting of leaf node. This splitting will require
 *addition of new leaf page number entry into the parent non-leaf, which may
 *in-turn get split. This may continue all the way upto the root causing the
 *root to get split. If root gets split, metapage needs to be changed
 *accordingly. Make sure to unpin pages as soon as you can.
 * @param key			Key to insert, pointer to integer/double/char
 *string
 * @param rid			Record ID of a record whose entry is getting
 *inserted into the index.
 **/

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
  // start recursive call
  PageId newPageNo = 0;
  int newIndex;
  recursiveInsert(this->rootPageNum, key, rid, newPageNo, newIndex,
                  this->rootPageNum == this->initialRootPageNum);

  // check whether need to split root
  if (newPageNo != 0) {
    splitRoot(newIndex, this->rootPageNum, newPageNo);
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void *lowValParm,
                                 const Operator lowOpParm,
                                 const void *highValParm,
                                 const Operator highOpParm) {
  // sanity check
  if ((lowOpParm != GT && lowOpParm != GTE) ||
      (highOpParm != LT && highOpParm != LTE)) {
    throw BadOpcodesException();
  }
  // load low value and high value
  // assumption: integer input
  this->lowValInt = *((int *)lowValParm);
  this->highValInt = *((int *)highValParm);
  this->lowOp = lowOpParm;
  this->highOp = highOpParm;

  if (this->lowValInt > highValInt) {
    this->lowValInt = 0;
    this->highValInt = 0;
    throw BadScanrangeException();
  }
  // printTree();

  // search for the correct leaf page containing the record that is smaller or
  // equal to lowValParm
  this->currentPageNum = this->getLeafPage(this->lowValInt);

  // this leaf page will get pinned until we are finished with this page
  Page *leafPage;
  bufMgr->readPage(this->file, this->currentPageNum, leafPage);
  this->currentPageData = leafPage;
  // printTree();

  this->nextEntry = this->getFirstIndex();
  this->scanExecuting = true;
}

const int BTreeIndex::getFirstIndex() {
  struct LeafNodeInt *leafNode = (struct LeafNodeInt *)this->currentPageData;
  for (int i = 0; i < leafNode->keyNum; i++) {
    if (this->lowOp == GT) {
      if (this->lowValInt < leafNode->keyArray[i]) {
        return i;
      }
    } else {
      if (this->lowValInt <= leafNode->keyArray[i]) {
        return i;
      }
    }
  }
  return leafNode->keyNum;
}

const PageId BTreeIndex::getLeafPage(const int key) {
  PageId levelOnePageId = this->getLevelOnePage(this->rootPageNum, key);
  Page *levelOnePage;
  bufMgr->readPage(file, levelOnePageId, levelOnePage);
  NonLeafNodeInt *node = (NonLeafNodeInt *)levelOnePage;
  assert(node->level == 1);

  for (int i = 0; i < node->keyNum; i++) {
    if (key < node->keyArray[i]) {
      PageId leafId = node->pageNoArray[i];
      bufMgr->unPinPage(file, levelOnePageId, false);
      return leafId;
    }
  }
  PageId leafId = node->pageNoArray[node->keyNum];
  bufMgr->unPinPage(file, levelOnePageId, false);
  return leafId;
}

const PageId BTreeIndex::getLevelOnePage(const PageId prev, const int key) {
  Page *prevPage;
  bufMgr->readPage(file, prev, prevPage);
  struct NonLeafNodeInt *prevNode = (struct NonLeafNodeInt *)prevPage;

  if (prevNode->level == 1) {
    bufMgr->unPinPage(this->file, prev, false);
    return prev;
  }

  for (int i = 0; i < prevNode->keyNum; i++) {
    if (key < prevNode->keyArray[i]) {
      PageId next = prevNode->pageNoArray[i];
      bufMgr->unPinPage(this->file, prev, false);
      return this->getLevelOnePage(next, key);
    }
  }
  PageId next = prevNode->pageNoArray[prevNode->keyNum];
  bufMgr->unPinPage(this->file, prev, false);
  return this->getLevelOnePage(next, key);
}
// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId &outRid) {
  if (this->scanExecuting == false) {
    throw ScanNotInitializedException();
  }
  // printTree();
  struct LeafNodeInt *leafNode = (struct LeafNodeInt *)this->currentPageData;
  if (this->nextEntry >= leafNode->keyNum) {
    // nextEntry is at the end of the node
    // switch to next node
    PageId rightNo = leafNode->rightSibPageNo;
    if (rightNo == 0) {
      throw IndexScanCompletedException();
    }
    bufMgr->unPinPage(this->file, this->currentPageNum, false);

    Page *nextPage;
    this->currentPageNum = rightNo;
    bufMgr->readPage(this->file, this->currentPageNum, nextPage);
    this->currentPageData = nextPage;
    this->nextEntry = 0;
  }
  // check if we reached higher bound
  if (this->highOp == LT) {
    if (leafNode->keyArray[this->nextEntry] >= this->highValInt) {
      throw IndexScanCompletedException();
    }
  } else {
    if (leafNode->keyArray[this->nextEntry] > this->highValInt) {
      // this->endScan();
      throw IndexScanCompletedException();
    }
  }

  outRid = leafNode->ridArray[this->nextEntry];
  this->nextEntry++;
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() {
  if (this->scanExecuting == false) {
    throw ScanNotInitializedException();
  }
  bufMgr->unPinPage(this->file, this->currentPageNum, false);
  this->nextEntry = 0;
  this->scanExecuting = false;
  this->currentPageData = NULL;
  this->currentPageNum = 0;
  this->lowValInt = 0;
  this->highValInt = 0;
}

void BTreeIndex::printTree() {
  printf("---------------Tree--------------\n");
  printf("Root: [%d]\n", this->rootPageNum);
  this->printTreeRecurs(0, this->rootPageNum,
                        this->rootPageNum == initialRootPageNum);
}

void BTreeIndex::printTreeRecurs(int level, PageId pageId, bool isleaf) {
  Page *p;
  bufMgr->readPage(file, pageId, p);
  if (isleaf) {
    LeafNodeInt *node = (LeafNodeInt *)p;
    std::cout << "leaf node: min = " << node->keyArray[0]
              << " max = " << node->keyArray[node->keyNum - 1]
              << " next = " << node->rightSibPageNo << std::endl;
    std::cout << "keyNum = " << node->keyNum << std::endl;
    for (int i = 0; i < node->keyNum; i++) {
      std::cout << node->keyArray[i] << " ";
    }
    std::cout << std::endl;
  } else {
    NonLeafNodeInt *node = (NonLeafNodeInt *)p;
    std::cout << "internal node:\n";
    for (int i = 0; i < node->keyNum; i++) {
      for (int j = 0; j < level; j++) std::cout << "--";
      printf("key[%d] -> Page[%d]\n", node->keyArray[i], node->pageNoArray[i]);
      this->printTreeRecurs(level + 1, node->pageNoArray[i], node->level == 1);
    }
    for (int j = 0; j < level; j++) std::cout << "--";
    std::cout << "Page[" << node->pageNoArray[node->keyNum] << "]" << std::endl;
    this->printTreeRecurs(level + 1, node->pageNoArray[node->keyNum],
                          node->level == 1);
    std::cout << "internal node end\n";
  }
  bufMgr->unPinPage(file, pageId, false);
}

void BTreeIndex::printNode(NonLeafNodeInt *newNode) {
  int rightLen = newNode->keyNum;
  for (int i = 0; i < rightLen; i++) {
    std::cout << newNode->keyArray[i] << " ";
  }
  std::cout << std::endl;
  for (int i = 0; i < rightLen; i++) {
    std::cout << newNode->pageNoArray[i] << " ";
  }
  std::cout << newNode->pageNoArray[rightLen] << std::endl;
}

}  // namespace badgerdb
