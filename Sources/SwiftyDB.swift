//
//  SwiftyDB.swift
//  SwiftyDB
//
//  Created by Øyvind Grimnes on 03/11/15.
//

#if os(Linux)
import Glibc
#endif
import Foundation
import tinysqlite


/** All objects in the database must conform to the 'Storable' protocol */
public protocol Storable : NSObjectProtocol {
    /** Used to initialize an object to get information about its properties */
    init()
    #if os(Linux)
    /* because of the lack of reflection and KVO-related methods, we need to explicitly map variables back */
    init(storable: [String: Value])
    #endif
}

/** Implement this protocol to use primary keys */
public protocol PrimaryKeys {
    /**
     Method used to define a set of primary keys for the types table
     
     - returns:  set of property names
     */
    static func primaryKeys() -> Set<String>
}

/** Implement this protocol to ignore arbitrary properties */
public protocol IgnoredProperties {
    /**
     Method used to define a set of ignored properties
     
     - returns:  set of property names
     */
    static func ignoredProperties() -> Set<String>
}


/** A class wrapping an SQLite3 database abstracting the creation of tables, insertion, update, and retrieval */
public class SwiftyDB {
    
    /** The database queue that will be used to access the database */
    private let databaseQueue : DatabaseQueue
    
    /** Path to the database that should be used */
    private let path: String
    
    /** A cache containing existing table names */
    private var existingTables: Set<String> = []
    
    
    /**
    Creates a new instance of SwiftyDB using a database in the documents directory. If the database does not exist, it will be created.
    
    - parameter databaseName:  name of the database
    
    - returns:                 an instance of SwiftyDB
    */
    
    public init(databaseName: String) {
    	#if os(Linux)
    	// TODO find a better place
    	let documentsDir : String = "~/.swiftydb/".stringByExpandingTildeInPath
    	print("using \(documentsDir) for the database")
        #else
        let documentsDir : String = NSSearchPathForDirectoriesInDomains(FileManager.SearchPathDirectory.documentDirectory, FileManager.SearchPathDomainMask.userDomainMask, true)[0]
        #endif
        path = documentsDir+"/\(databaseName).sqlite"
        
        databaseQueue = DatabaseQueue(path: path)
    }
    
    public init(name: String, directory: String) {
        path = directory+"/\(name).sqlite"
        
        databaseQueue = DatabaseQueue(path: path)
    }
    
    public init(path: String) {
        self.path = path
        
        databaseQueue = DatabaseQueue(path: path)
    }
    
    // MARK: - Database operations
    
    /**
    Add an object to the database
    
    - parameter object:     object to be added to the database
    - parameter update:     indicates whether the record should be updated if already present
    
    - returns:              Result type indicating the success of the query
    */
    
    public func addObject <S: Storable> (object: S, update: Bool = true) -> Result<Bool> {
        return self.addObjects(objects: [object], update: update)
    }
    
    /**
     Add objects to the database
     
     - parameter objects:    objects to be added to the database
     - parameter update:     indicates whether the record should be updated if already present
     
     - returns:              Result type indicating the success of the query
     */
    
    public func addObjects <S: Storable> (objects: [S], update: Bool = true) -> Result<Bool> {
        guard objects.count > 0 else {
            return Result.Success(true)
        }
        
        do {
            if !(try tableExistsForType(type: S.self)) {
                createTableForTypeRepresentedByObject(object: objects.first!)
            }
            
            let insertStatement = StatementGenerator.insertStatementForType(type: S.self, update: update)
            
            try databaseQueue.transaction { (database) -> Void in
                let statement = try database.prepare(query: insertStatement)
                
                defer {
                    /* If an error occurs, try to finalize the statement */
                    let _ = try? statement.finalize()
                }
                
                for object in objects {
                    let data = self.dataFromObject(object: object)
                    try statement.executeUpdate(namedValues: data)
                }
            }
        } catch let error {
            return Result.Error(error)
        }
        
        return Result.Success(true)
    }
    
    /**
     Add objects to the database
     
     - parameter object:        object to be added to the database
     - parameter moreObjects:   more objects to be added to the database
     
     - returns:                 Result type indicating the success of the query
     */
    
    public func addObjects <S: Storable> (object: S, _ moreObjects: S...) -> Result<Bool> {
        return addObjects(objects: [object] + moreObjects)
    }
    
    /**
     Remove objects of a specified type, matching a filter, from the database
     
     - parameter filter:   `Filter` object containing the filters for the query
     - parameter type:      type of the objects to be deleted
     
     - returns:             Result type indicating the success of the query
     */
    
    public func deleteObjectsForType (type: Storable.Type, matchingFilter filter: Filter? = nil) -> Result<Bool> {
        do {
            guard try tableExistsForType(type: type) else {
                return Result.Success(true)
            }
            
            let deleteStatement = StatementGenerator.deleteStatementForType(type: type, matchingFilter: filter)
            
            try databaseQueue.database { (database) -> Void in
                try database.prepare(query: deleteStatement)
                    .executeUpdate(namedValues: filter?.parameters() ?? [:])
                            .finalize()
            }
        } catch let error {
            return .Error(error)
        }
        
        return .Success(true)
    }
    
    /**
     Get data for a specified type, matching a filter, from the database
     
     - parameter filter:    `Filter` object containing the filters for the query
     - parameter type:      type of the objects for which to retrieve data
     
     - returns:             Result type wrapping an array with the dictionaries representing objects, or an error if unsuccessful
     */
    
    public func dataForType <S: Storable> (type: S.Type, matchingFilter filter: Filter? = nil) -> Result<[[String: Value?]]> {
        
        var results: [[String: Value?]] = []
        do {
            guard try tableExistsForType(type: type) else {
                return Result.Success([])
            }
            
            /* Generate statement */
            let query = StatementGenerator.selectStatementForType(type: type, matchingFilter: filter)
            
            try databaseQueue.database { (database) -> Void in
                let parameters = filter?.parameters() ?? [:]
                let statement = try! database.prepare(query: query)
                    .execute(namedValues: parameters)
                
                /* Create a dummy object used to extract property data */
                let object = type.init()
                let objectPropertyData = PropertyData.validPropertyDataForObject(object: object)
                
                results = statement.map { row in
                    self.parsedDataForRow(row: row, forPropertyData: objectPropertyData)
                }
                
                try statement.finalize()
            }
        } catch let error {
            return .Error(error)
        }
        
        return .Success(results)
    }
    
    
    /**
     Get data for a specified type, matching a filter, from the database
     
     - parameter filter:    `Filter` object containing the filters for the query
     - parameter type:      type of the objects for which to retrieve data
     - parameter limit:     max number of lines to get
     - parameter offset:    offsets the returned rows (especially useful with limit)
     - parameter orderBy:   pre-sort the returned data according to that property/column
     
     - returns:             Result type wrapping an array with the dictionaries representing objects, or an error if unsuccessful
     */
    
    public func dataForType <S: Storable> (type: S.Type, matchingFilter filter: Filter? = nil, limit: Int? = nil, offset: Int? = nil, orderBy: String? = nil) -> Result<[[String: Value?]]> {
        
        var results: [[String: Value?]] = []
        do {
            guard try tableExistsForType(type: type) else {
                return Result.Success([])
            }
            
            /* Generate statement */
            let query = StatementGenerator.selectStatementForType(type: type, matchingFilter: filter, limit: limit, offset: offset, orderBy: orderBy)
            
            try databaseQueue.database { (database) -> Void in
                let parameters = filter?.parameters() ?? [:]
                let statement = try! database.prepare(query: query)
                    .execute(namedValues: parameters)
                
                /* Create a dummy object used to extract property data */
                let object = type.init()
                let objectPropertyData = PropertyData.validPropertyDataForObject(object: object)
                
                results = statement.map { row in
                    self.parsedDataForRow(row: row, forPropertyData: objectPropertyData)
                }
                
                try statement.finalize()
            }
        } catch let error {
            return .Error(error)
        }
        
        return .Success(results)
    }
    
    
    
// MARK: - Private functions
    
    /**
    Creates a new table for the specified type based on the provided column definitions
    
    The parameter is an object, instead of a type, to avoid forcing the user to implement initialization methods such as 'init'
    
    - parameter type:   type of objects data in the table represents
    
    - returns:          Result type indicating the success of the query
    */
    
    @discardableResult private func createTableForTypeRepresentedByObject <S: Storable> (object: S) -> Result<Bool> {
        
        let statement = StatementGenerator.createTableStatementForTypeRepresentedByObject(object: object)
        
        do {
            try databaseQueue.database(block: { (database) -> Void in
                try database.prepare(query: statement)
                            .executeUpdate()
                            .finalize()
            })
        } catch let error {
            return .Error(error)
        }
        
        existingTables.insert(tableNameForType(type: S.self))
        
        return .Success(true)
    }
    
    /**
     Serialize the object
     
     - parameter object:    object containing the data to be extracted
     
     - returns:             dictionary containing the data from the object
     */
    
    private func dataFromObject (object: Storable) -> [String: SQLiteValue?] {
        var dictionary: [String: SQLiteValue?] = [:]
        
        for propertyData in PropertyData.validPropertyDataForObject(object: object) {
            dictionary[propertyData.name!] = propertyData.value as? SQLiteValue
        }
        
        return dictionary
    }
    
    /**
     Check whether a table representing a type exists, or not
     
     - parameter type:  type implementing the Storable protocol
     
     - returns:         boolean indicating if the table exists
     */
    
    private func tableExistsForType(type: Storable.Type) throws -> Bool {
        let tableName = tableNameForType(type: type)
        
        var exists: Bool = existingTables.contains(tableName)
        
        /* Return true if the result is cached */
        guard !exists else {
            return exists
        }
        
        try databaseQueue.database(block: { (database) in
            exists = try database.containsTable(tableName: tableName)
        })
        
        /* Cache the result */
        if exists {
            existingTables.insert(tableName)
        }
        
        return exists
    }
    
    /**
     Used to create name of the table representing a type
     
     - parameter type:  type for which to generate a table name
     
     - returns:         table name as a String
     */
    private func tableNameForType(type: Storable.Type) -> String {
        return String(describing: type)
    }
    
    /**
     Create a dictionary with values matching datatypes based on some property data
     
     - parameter row:           row, in the form of a wrapped SQLite statement, from which to receive values
     - parameter propertyData:  array containing information about property names and datatypes
     
     - returns:                 dictionary containing data of types matching the properties of the target type
     */
    
    private func parsedDataForRow(row: Statement, forPropertyData propertyData: [PropertyData]) -> [String: Value?] {
        var rowData: [String: Value?] = [:]
        
        for propertyData in propertyData {
            rowData[propertyData.name!] = valueForProperty(propertyData: propertyData, inRow: row)
        }
        
        return rowData
    }
    
    /**
     Retrieve the value for a property with the correct datatype
     
     - parameter propertyData:  object containing information such as property name and type
     - parameter row:           row, in the form of a wrapped SQLite statement, from which to retrieve the value
     
     - returns:                 optional value for the property
     */
    
    private func valueForProperty(propertyData: PropertyData, inRow row: Statement) -> Value? {
        if row.typeForColumn(name: propertyData.name!) == .Null {
            return nil
        }
        
        switch propertyData.type {
        case is Date.Type:    return row.sdateForColumn(name: propertyData.name!) as? Value
        case is NSDate.Type:    return row.dateForColumn(name: propertyData.name!) as? Value
        case is Data.Type:    return row.sdataForColumn(name: propertyData.name!) as? Value
        case is NSData.Type:    return row.dataForColumn(name: propertyData.name!) as? Value
        case is NSNumber.Type:  return row.numberForColumn(name: propertyData.name!) as? Value
            
        case is String.Type:    return row.stringForColumn(name: propertyData.name!) as? Value
        case is NSString.Type:  return row.nsstringForColumn(name: propertyData.name!) as? Value
        case is Character.Type: return row.characterForColumn(name: propertyData.name!) as? Value
            
        case is Double.Type:    return row.doubleForColumn(name: propertyData.name!) as? Value
        case is Float.Type:     return row.floatForColumn(name: propertyData.name!) as? Value
            
        case is Int.Type:       return row.integerForColumn(name: propertyData.name!) as? Value
        case is Int8.Type:      return row.integer8ForColumn(name: propertyData.name!) as? Value
        case is Int16.Type:     return row.integer16ForColumn(name: propertyData.name!) as? Value
        case is Int32.Type:     return row.integer32ForColumn(name: propertyData.name!) as? Value
        case is Int64.Type:     return row.integer64ForColumn(name: propertyData.name!) as? Value
        case is UInt.Type:      return row.unsignedIntegerForColumn(name: propertyData.name!) as? Value
        case is UInt8.Type:     return row.unsignedInteger8ForColumn(name: propertyData.name!) as? Value
        case is UInt16.Type:    return row.unsignedInteger16ForColumn(name: propertyData.name!) as? Value
        case is UInt32.Type:    return row.unsignedInteger32ForColumn(name: propertyData.name!) as? Value
        case is UInt64.Type:    return row.unsignedInteger64ForColumn(name: propertyData.name!) as? Value
            
        case is Bool.Type:      return row.boolForColumn(name: propertyData.name!) as? Value
            
        case is NSArray.Type:  
            return NSKeyedUnarchiver.unarchiveObject(with: row.sdataForColumn(name: propertyData.name!)! as Data) as? NSArray
        case is NSDictionary.Type:
             return NSKeyedUnarchiver.unarchiveObject(with: row.sdataForColumn(name: propertyData.name!)! as Data) as? NSDictionary
            
        default:                return nil
        }
    }
}



extension SwiftyDB {
    
// MARK: - Dynamic initialization
    
    /**
     Get objects of a specified type, matching a filter, from the database
     
     - parameter filter:   `Filter` object containing the filters for the query
     - parameter type:      type of the objects to be retrieved
     
     - returns:             Result wrapping the objects, or an error, if unsuccessful
     */
    
    public func objectsForType <D : Storable> (type: D.Type, matchingFilter filter: Filter? = nil) -> Result<[D]> where D: NSObject {
        let dataResults = dataForType(type: D.self, matchingFilter: filter)
        
        if !dataResults.isSuccess {
            return .Error(dataResults.error!)
        }
        
        let objects: [D] = dataResults.value!.map {
            objectWithData(data: $0 as! [String: Value], forType: D.self)
        }
        
        return .Success(objects)
    }
    
    /**
     Get objects of a specified type, matching a filter, from the database
     
     - parameter filter:   `Filter` object containing the filters for the query
     - parameter type:      type of the objects to be retrieved
     - parameter limit:     limits the amount of data returned to limit rows
     - parameter offset:    offsets the amount of data returned (particularly useful in conjunction with limit)
     - parameter orderBy:   pre-orders the output data
     
     - returns:             Result wrapping the objects, or an error, if unsuccessful
     */
    public func objectsForType <D : Storable> (type: D.Type, matchingFilter filter: Filter? = nil, limit: Int? = nil, offset: Int? = nil, orderBy: String? = nil) -> Result<[D]> where D: NSObject {
    let dataResults = dataForType(type: D.self, matchingFilter: filter, limit: limit, offset: offset,orderBy: orderBy)
    
        if !dataResults.isSuccess {
            return .Error(dataResults.error!)
        }
        
        let objects: [D] = dataResults.value!.map {
            objectWithData(data: $0 as! [String: Value], forType: D.self)
        }
        
        return .Success(objects)
    }
    
    /**
     Creates a new dynamic object of a specified type and populates it with data from the provided dictionary
     
     - parameter data:   dictionary containing data
     - parameter type:   type of object the data represents
     
     - returns:          object of the provided type populated with the provided data
     */
    
    private func objectWithData <D : Storable> (data: [String: Value], forType type: D.Type) -> D where D: NSObject {
        
        #if os(Linux)
        let object = D.init(storable: data)
        #else
       	let object = (type as NSObject.Type).init() as! D
        object.setValuesForKeys(data)
		#endif

        return object
    }
}

#if os(Linux)
extension String {
	var stringByExpandingTildeInPath : String {
		guard let out = getenv("HOME") else { return String(self) }
        guard let homepath = String(validatingUTF8: out) else { return String(self) }
        
        return self.replacingOccurrences(of: "~", with: homepath)
	}
}
#endif
