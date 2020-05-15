/**
 * Purpose: This file tests buffer manager
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
#include <stdlib.h>
//#include <stdio.h>
#include <cstring>
#include <memory>
#include "page.h"
#include "buffer.h"
#include "file_iterator.h"
#include "page_iterator.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/invalid_record_exception.h"

#define PRINT_ERROR(str)                                                                                               \
  {                                                                                                                    \
    std::cerr << "On Line No:" << __LINE__ << "\n";                                                                    \
    std::cerr << str << "\n";                                                                                          \
    exit(1);                                                                                                           \
  }

#include <sys/types.h>
#include <unistd.h>
#include <sys/wait.h>

int fork_test(void (*test)())
{
  pid_t pid = fork();
  if (pid == -1)
    perror("Cannot run tests");

  if (pid)
  {
    int wstatus;
    waitpid(pid, &wstatus, 0);
    return WEXITSTATUS(wstatus);
  }
  else
  {
    // no return!
    test();
    exit(0);
  }
}

using namespace badgerdb;

const PageId num = 100;
PageId pid[num], pageno1, pageno2, pageno3, pageno6, i;
RecordId rid[num], rid2, rid3, rid6;
Page *page, *page2, *page3;
char tmpbuf[100];
BufMgr *bufMgr;
File *file1ptr, *file2ptr, *file3ptr, *file4ptr, *file5ptr, *file6ptr;

void test1();
void test2();
void test3();
void test4();
void test5();
void test6();
void test7();
void test8();
void test9();
void test10();
void test11();
void testBufMgr();

int main()
{
  // Following code shows how to you File and Page classes

  const std::string &filename = "test.db";
  // Clean up from any previous runs that crashed.
  try
  {
    File::remove(filename);
  }
  catch (FileNotFoundException &)
  {
  }

  {
    // Create a new database file.
    File new_file = File::create(filename);

    // Allocate some pages and put data on them.
    PageId third_page_number;
    for (int i = 0; i < 5; ++i)
    {
      Page new_page = new_file.allocatePage();
      if (i == 2)
      {
        // Keep track of the identifier for the third page so we can read it
        // later.
        third_page_number = new_page.page_number();
      }
      new_page.insertRecord("hello!");
      // Write the page back to the file (with the new data).
      new_file.writePage(new_page);
    }

    // Iterate through all pages in the file.
    for (FileIterator iter = new_file.begin(); iter != new_file.end(); ++iter)
    {
      // every time `iter` is dereferenced, a new in-memory copy of the page is created
      // make sure it does not go out of scope
      auto page = *iter;
      // Iterate through all records on the page.
      for (PageIterator page_iter = page.begin(); page_iter != page.end(); ++page_iter)
      {
        std::cout << "Found record: " << *page_iter << " on page " << page.page_number() << "\n";
      }
    }

    // Retrieve the third page and add another record to it.
    Page third_page = new_file.readPage(third_page_number);
    const RecordId &rid = third_page.insertRecord("world!");
    new_file.writePage(third_page);

    // Retrieve the record we just added to the third page.
    std::cout << "Third page has a new record: " << third_page.getRecord(rid) << "\n\n";
  }
  // new_file goes out of scope here, so file is automatically closed.

  // Delete the file since we're done with it.
  File::remove(filename);

  // This function tests buffer manager, comment this line if you don't wish to test buffer manager
  testBufMgr();
}

void testBufMgr()
{
  // create buffer manager
  bufMgr = new BufMgr(num);

  // create dummy files
  const std::string &filename1 = "test.1";
  const std::string &filename2 = "test.2";
  const std::string &filename3 = "test.3";
  const std::string &filename4 = "test.4";
  const std::string &filename5 = "test.5";
  const std::string &filename6 = "test.6";

  try
  {
    File::remove(filename1);
    File::remove(filename2);
    File::remove(filename3);
    File::remove(filename4);
    File::remove(filename5);
    File::remove(filename6);
  }

  catch (FileNotFoundException &e)
  {
  }

  File file1 = File::create(filename1);
  File file2 = File::create(filename2);
  File file3 = File::create(filename3);
  File file4 = File::create(filename4);
  File file5 = File::create(filename5);
  File file6 = File::create(filename6);

  file1ptr = &file1;
  file2ptr = &file2;
  file3ptr = &file3;
  file4ptr = &file4;
  file5ptr = &file5;
  file6ptr = &file6;

  // Test buffer manager
  // Comment tests which you do not wish to run now. Tests are dependent on their preceding tests. So, they have to be
  // run in the following order. Commenting  a particular test requires commenting all tests that follow it else those
  // tests would fail.

  fork_test(test1);
  fork_test(test2);
  fork_test(test3);
  fork_test(test4);
  fork_test(test5);
  fork_test(test6);
  fork_test(test7);
  fork_test(test8);
  fork_test(test9);
  fork_test(test10);
  fork_test(test11);

  // Close files before deleting them
  file1.close();
  file2.close();
  file3.close();
  file4.close();
  file5.close();
  file6.close();

  // Delete files
  File::remove(filename1);
  File::remove(filename2);
  File::remove(filename3);
  File::remove(filename4);
  File::remove(filename5);
  File::remove(filename6);

  delete bufMgr;

  std::cout << "\n"
            << "Passed all tests."
            << "\n";
}

void test1()
{
  // Allocating pages in a file...
  for (i = 0; i < num; i++)
  {
    bufMgr->allocPage(file1ptr, pid[i], page);
    sprintf((char *)tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
    rid[i] = page->insertRecord(tmpbuf);
    bufMgr->unPinPage(file1ptr, pid[i], true);
  }

  // Reading pages back...
  for (i = 0; i < num; i++)
  {
    bufMgr->readPage(file1ptr, pid[i], page);
    sprintf((char *)&tmpbuf, "test.1 Page %d %7.1f", pid[i], (float)pid[i]);
    if (strncmp(page->getRecord(rid[i]).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
    {
      PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
    }
    bufMgr->unPinPage(file1ptr, pid[i], false);
  }
  std::cout << "Test 1 passed"
            << "\n";
}

void test2()
{
  // Writing and reading back multiple files
  // The page number and the value should match

  for (i = 0; i < num / 3; i++)
  {
    bufMgr->allocPage(file2ptr, pageno2, page2);
    sprintf((char *)tmpbuf, "test.2 Page %d %7.1f", pageno2, (float)pageno2);
    rid2 = page2->insertRecord(tmpbuf);

    bufMgr->allocPage(file3ptr, pageno3, page3);
    sprintf((char *)tmpbuf, "test.3 Page %d %7.1f", pageno3, (float)pageno3);
    rid3 = page3->insertRecord(tmpbuf);

    bufMgr->readPage(file2ptr, pageno2, page2);
    sprintf((char *)&tmpbuf, "test.2 Page %d %7.1f", pageno2, (float)pageno2);
    if (strncmp(page2->getRecord(rid2).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
    {
      PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
    }

    bufMgr->readPage(file3ptr, pageno3, page3);
    sprintf((char *)&tmpbuf, "test.3 Page %d %7.1f", pageno3, (float)pageno3);
    if (strncmp(page3->getRecord(rid3).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
    {
      PRINT_ERROR("ERROR :: CONTENTS DID NOT MATCH");
    }

    bufMgr->unPinPage(file1ptr, pageno1, false);
  }

  for (i = 0; i < num / 3; i++)
  {
    bufMgr->unPinPage(file2ptr, i + 1, true);
    bufMgr->unPinPage(file2ptr, i + 1, true);
    bufMgr->unPinPage(file3ptr, i + 1, true);
    bufMgr->unPinPage(file3ptr, i + 1, true);
  }

  std::cout << "Test 2 passed"
            << "\n";
}

void test3()
{
  try
  {
    bufMgr->readPage(file4ptr, 1, page);
    PRINT_ERROR(
        "ERROR :: File4 should not exist. Exception should have been thrown before execution reaches this point.");
  }
  catch (InvalidPageException &e)
  {
  }

  std::cout << "Test 3 passed"
            << "\n";
}

void test4()
{
  bufMgr->allocPage(file4ptr, i, page);
  bufMgr->unPinPage(file4ptr, i, true);
  try
  {
    bufMgr->unPinPage(file4ptr, i, false);
    PRINT_ERROR(
        "ERROR :: Page is already unpinned. Exception should have been thrown before execution reaches this point.");
  }
  catch (PageNotPinnedException &e)
  {
  }

  std::cout << "Test 4 passed"
            << "\n";
}

void test5()
{
  for (i = 0; i < num; i++)
  {
    bufMgr->allocPage(file5ptr, pid[i], page);
    sprintf((char *)tmpbuf, "test.5 Page %d %7.1f", pid[i], (float)pid[i]);
    rid[i] = page->insertRecord(tmpbuf);
  }

  PageId tmp;
  try
  {
    bufMgr->allocPage(file5ptr, tmp, page);
    PRINT_ERROR("ERROR :: No more frames left for allocation. Exception should have been thrown before execution "
                "reaches this point.");
  }
  catch (BufferExceededException &e)
  {
  }

  std::cout << "Test 5 passed"
            << "\n";

  for (i = 1; i <= num; i++)
    bufMgr->unPinPage(file5ptr, i, true);
}

void test6()
{
  // flushing file with pages still pinned. Should generate an error
  for (i = 1; i <= num; i++)
  {
    bufMgr->readPage(file1ptr, i, page);
  }

  try
  {
    bufMgr->flushFile(file1ptr);
    PRINT_ERROR("ERROR :: Pages pinned for file being flushed. Exception should have been thrown before execution "
                "reaches this point.");
  }
  catch (PagePinnedException &e)
  {
  }

  std::cout << "Test 6 passed"
            << "\n";
  for (i = 1; i <= num; i++)
    bufMgr->unPinPage(file1ptr, i, true);
  bufMgr->flushFile(file1ptr);
}

void test7()
{
  // test for alloc buffer if the page numbers are correct for single file
  for (i = 1; i <= num; i++)
  {
    bufMgr->readPage(file1ptr, i, page);
    if (page->page_number() != i)
    {
      PRINT_ERROR("ERROR :: Read page returned page with wrong page number.");
    }
    if (bufMgr->bufPool[i - 1].page_number() != i)
    {
      printf("i: %d, bufPool: %d\n", i, bufMgr->bufPool[i].page_number());
      PRINT_ERROR("ERROR :: allocBuffer alloced wrong frame number.");
    }
  }

  // test clock algorithm
  const PageId toUnPin = 2;
  const PageId toRead = 3;
  bufMgr->unPinPage(file1ptr, toUnPin, false);
  bufMgr->readPage(file2ptr, toRead, page);

  if (bufMgr->bufPool[toUnPin - 1].page_number() != toRead)
  {
    PRINT_ERROR("ERROR :: Clock algorithm wrong");
  }
  std::cout << "Test 7 passed"
            << "\n";

  bufMgr->unPinPage(file2ptr, toRead, false);
  for (i = 1; i <= num; i++)
    bufMgr->unPinPage(file1ptr, i, false);
}

void test8()
{
  // test for readPage
  bufMgr->allocPage(file2ptr, pageno2, page2);
  sprintf((char *)tmpbuf, "test.8 Page %d %7.1f", pageno2, (float)pageno2);
  rid2 = page2->insertRecord(tmpbuf);
  bufMgr->unPinPage(file2ptr, pageno2, false);

  // fill the pool with pages to
  // flush out the dirty page of file2ptr
  for (i = 1; i <= num; i++)
  {
    bufMgr->readPage(file1ptr, i, page);
  }
  for (i = 1; i <= num; i++)
    bufMgr->unPinPage(file1ptr, i, false);

  // read the page again from file and check if the record is flushed
  bufMgr->readPage(file2ptr, pageno2, page2);
  try
  {
    if (strncmp(page2->getRecord(rid2).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
      // nothing
      PRINT_ERROR("ERROR :: dirty bit was set to false, but the page was still flushed")
  }
  catch (const InvalidRecordException &e)
  {
    // should receive exception as the record is not written
    std::cout << "Test 8 passed"
              << "\n";
  }
}

void test9()
{
  // test for unpinPage
  // unpin the same page twice
  const PageId toRead = 1;
  try
  {
    bufMgr->readPage(file1ptr, toRead, page);
    bufMgr->unPinPage(file1ptr, toRead, true);

    bufMgr->unPinPage(file1ptr, toRead, false);
    PRINT_ERROR("ERROR :: unpin page twice, should throw PageNotPinnedException");
  }
  catch (const PageNotPinnedException &e)
  {
    std::cout << "Test 9 passed"
              << "\n";
  }
}

void test10()
{
  // test for allocPage
  // allocate two pages for file1 and check their free space
  bufMgr->allocPage(file1ptr, pageno1, page);
  bufMgr->allocPage(file1ptr, pageno2, page2);
  if (page->getFreeSpace() != page2->getFreeSpace())
  {
    PRINT_ERROR("ERROR :: New pages in a file do not have the same free space");
  }

  std::cout << "Test 10 passed"
            << "\n";

  bufMgr->unPinPage(file1ptr, pageno1, false);
  bufMgr->unPinPage(file1ptr, pageno2, false);
}

void test11()
{
  // TODO: test for flushFile
  // modify page
  for (i = 0; i < num - 1; i++)
  {
    bufMgr->allocPage(file6ptr, pageno6, page);
    sprintf((char *)tmpbuf, "Hello World");
    rid6 = page->insertRecord(tmpbuf);
  }

  // current unpinned page won't be overriden because more than one availble frame
  bufMgr->unPinPage(file6ptr, num - 1, true);
  bufMgr->allocPage(file6ptr, pageno6, page);

  // current unpinned frame must be overriden because only one frame left
  bufMgr->allocPage(file6ptr, pageno6, page);

  // upin page#num(frame num-1) and dispose #num+1(frame num-2)
  // then read page#num-1 back, clock will go num-1->num-2->num-1
  // page#num should replace by page#num-1
  bufMgr->unPinPage(file6ptr, num, true);
  bufMgr->disposePage(file6ptr, num + 1);
  bufMgr->readPage(file6ptr, num - 1, page);

  // read page#num back to override page#num+1
  bufMgr->readPage(file6ptr, num, page);

  // now all buffer frame 0~num-1 are used where page = 1~num
  for (i = 1; i <= num; i++)
  {
    // all pages are in the buffer pool which can be read without allocating new page
    // because buffer is full, it will casue exception if any page is not in the buffer
    bufMgr->readPage(file6ptr, i, page);
  }
  // unpin all pages and flush buffer
  for (i = 0; i <= num; i++)
  {
    bufMgr->unPinPage(file6ptr, i, false);
    bufMgr->unPinPage(file6ptr, i, true);
  }
  bufMgr->flushFile(file6ptr);

  // check the correctness of the previous flush and dirty bit
  for (i = 1; i < num; i++)
  {
    rid6.page_number = i;
    rid6.slot_number = 1;
    bufMgr->readPage(file6ptr, i, page);
    sprintf((char *)&tmpbuf, "Hello World");
    if (strncmp(page->getRecord(rid6).c_str(), tmpbuf, strlen(tmpbuf)) != 0)
    {
      PRINT_ERROR(
          "Error :: Content do not match. FlushFile should flush the page back to the disk if the page is dirty.");
    }
  }
  std::cout << "Test 11 passed"
            << "\n";
}
