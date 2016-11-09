#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"

#define SLOT_SIZE 256

// Bookkeeping for scans
typedef struct RM_ScanHandle
{
  RM_TableData *rel;
  int currentPage;
  int currentSlot;
  Expr *expr;
  void *mgmtData;
} RM_ScanHandle;

// table and manager
extern RC initRecordManager (void *mgmtData);
extern RC shutdownRecordManager ();
extern RC createTable (char *name, Schema *schema);
extern RC openTable (RM_TableData *rel, char *name);
extern RC closeTable (RM_TableData *rel);
extern RC deleteTable (char *name);
extern int getNumTuples (RM_TableData *rel);

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern RC next (RM_ScanHandle *scan, Record *record);
extern RC closeScan (RM_ScanHandle *scan);

// dealing with schemas
extern int getRecordSize (Schema *schema);
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
extern RC freeSchema (Schema *schema);

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema);
extern RC freeRecord (Record *record);
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

// additional function
extern RC addPageMetadataBlock(SM_FileHandle *fh);
extern int getFileMetaDataSize(BM_BufferPool *bm);
extern int recordCostSlot(BM_BufferPool *bm);
extern int getSlotSize(BM_BufferPool *bm);
#endif // RECORD_MGR_H

// addtional by xpy:
extern int getTotalRecords_slot( RM_TableData *rel);
extern int modifyFileMetaPage (RM_TableData *rel, char *name);
extern int File_Meta_Data_Size(BM_BufferPool *bm);
