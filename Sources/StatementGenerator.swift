//
//  QueryGenerator.swift
//  SwiftyDB
//
//  Created by Øyvind Grimnes on 27/12/15.
//

import Foundation
import tinysqlite
internal enum SQLiteDatatype: String {
    case Text       = "TEXT"
    case Integer    = "INTEGER"
    case Real       = "REAL"
    case Blob       = "BLOB"
    case Numeric    = "NUMERIC"
    case Null       = "NULL"
    
    init?(type: Value.Type) {
        switch type {
        case is Int.Type, is Int8.Type, is Int16.Type, is Int32.Type, is Int64.Type, is UInt.Type, is UInt8.Type, is UInt16.Type, is UInt32.Type, is UInt64.Type, is Bool.Type:
            self.init(rawValue: "INTEGER")
        case is Double.Type, is Float.Type, is NSDate.Type, is Date.Type:
            self.init(rawValue: "REAL")
        case is NSData.Type, is Data.Type:
            self.init(rawValue: "BLOB")
        case is NSNumber.Type:
            self.init(rawValue: "NUMERIC")
        case is String.Type, is NSString.Type, is Character.Type:
            self.init(rawValue: "TEXT")
        case is NSArray.Type, is NSDictionary.Type:
            self.init(rawValue: "BLOB")
        default:
            fatalError("The datatype was not defined")
        }
    }
}

internal class StatementGenerator {
    
    internal class func createTableStatementForTypeRepresentedByObject <S: Storable> (object: S) -> String {
        
        var statement = "CREATE TABLE " + tableNameForType(type: S.self) + " ("
        
        /* Define all the columns of the table */
        var columnDefinitions: [String] = []
        
        for propertyData in PropertyData.validPropertyDataForObject(object: object) {
            var columnDefinition = "'\(propertyData.name!)' \(SQLiteDatatype(type: propertyData.type!)!.rawValue)"
            columnDefinition += propertyData.isOptional ? "" : " NOT NULL"
            
            columnDefinitions.append(columnDefinition)
        }
        
        statement += columnDefinitions.joined(separator: ", ")
        
        /* Add a primary key constraint if provided */
        if let primaryKeysType = S.self as? PrimaryKeys.Type {
            statement += ", PRIMARY KEY (\(primaryKeysType.primaryKeys().joined(separator: ", ")))"
        }
        
        /* Conclude the statement and return */
        statement += ")"

        return statement
    }
    
    internal class func insertStatementForType(type: Storable.Type, update: Bool) -> String {
        var statement = "INSERT OR " + (update ? "REPLACE" : "ABORT") + " INTO " + tableNameForType(type: type)
        
        let propertyData = PropertyData.validPropertyDataForObject(object: type.init())
        
        let columns = propertyData.map {"'\($0.name!)'"}
        let namedParameters = propertyData.map {":\($0.name!)"}
        
        /* Columns to be inserted */
        statement += " (" + columns.joined(separator: ", ") + ") "
        
        /* Values to be inserted */
        statement += "VALUES (" + namedParameters.joined(separator: ", ") + ")"
        
        return statement
    }
    
    internal class func selectStatementForType(type: Storable.Type, matchingFilter filter: Filter?) -> String {
        
        let tableName =  tableNameForType(type: type)
        
        var statement = "SELECT ALL * FROM \(tableName)"
        
        guard filter != nil else {
            return statement
        }
        
        statement += " " + filter!.whereStatement()
        
        return statement
    }
    
    internal class func selectStatementForType(type: Storable.Type, matchingFilter filter: Filter?, limit: Int? = nil, offset: Int? = nil, orderBy: String? = nil) -> String {
        
        var statement = selectStatementForType(type: type, matchingFilter: filter)
        
        if orderBy != nil {
            statement += " ORDER BY " + orderBy! + " ASC"
        }
        if limit != nil {
            statement += " LIMIT " + String(limit!)
        }
        if offset != nil {
            statement += " OFFSET " + String(offset!)
        }
        
        return statement
    }
    
   internal class func deleteStatementForType(type: Storable.Type, matchingFilter filter: Filter?) -> String {
        
        let tableName =  tableNameForType(type: type)
        
        var statement = "DELETE FROM \(tableName)"
        
        guard filter != nil else {
            return statement
        }
                
        statement += " \(filter!.whereStatement())"
        
        return statement
    }
    
    
    
    /** Name of the table representing a class */
    private class func tableNameForType(type: Storable.Type) -> String {
        return String(describing: type)
    }
}
