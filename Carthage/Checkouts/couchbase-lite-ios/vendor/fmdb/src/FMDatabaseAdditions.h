//
//  FMDatabaseAdditions.h
//  fmkit
//
//  Created by August Mueller on 10/30/05.
//  Copyright 2005 Flying Meat Inc.. All rights reserved.
//

#import <Foundation/Foundation.h>
@interface CBL_FMDatabase (FMDatabaseAdditions)


- (int)intForQuery:(NSString*)objs, ...;
- (long long)longLongForQuery:(NSString*)objs, ...;
- (BOOL)boolForQuery:(NSString*)objs, ...;
- (double)doubleForQuery:(NSString*)objs, ...;
- (NSString*)stringForQuery:(NSString*)objs, ...; 
- (NSData*)dataForQuery:(NSString*)objs, ...;

// Notice that there's no dataNoCopyForQuery:.
// That would be a bad idea, because we close out the result set, and then what
// happens to the data that we just didn't copy?  Who knows, not I.

#if 0 // Unused in Couchbase Lite --jens
- (long)longForQuery:(NSString*)objs, ...; 
- (NSDate*)dateForQuery:(NSString*)objs, ...;
- (BOOL)tableExists:(NSString*)tableName;
- (CBL_FMResultSet*)getSchema;
- (CBL_FMResultSet*)getTableSchema:(NSString*)tableName;
- (BOOL)columnExists:(NSString*)tableName columnName:(NSString*)columnName;

- (BOOL)validateSQL:(NSString*)sql error:(NSError**)error;
#endif
@end
