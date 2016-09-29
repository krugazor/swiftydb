//
//  SwiftyDB+Asynchronous.swift
//  SwiftyDB
//
//  Created by Øyvind Grimnes on 13/01/16.
//

import tinysqlite
import Foundation
import Dispatch

/** Support asynchronous queries */
extension SwiftyDB {

    /** A global, concurrent queue with default priority */
    var queue: DispatchQueue {
        return DispatchQueue.global(qos: .userInitiated)
    }

// MARK: - Asynchronous database operations

    /**
     Asynchronously add object to the database

     - parameter object:    object to be added to the database
     - parameter update:    indicates whether the record should be updated if already present
     */

    public func asyncAddObject <S: Storable> (object: S, update: Bool = true, withCompletionHandler completionHandler: ((Result<Bool>)->Void)? = nil) {
        asyncAddObjects(objects: [object], update: update, withCompletionHandler: completionHandler)
    }

    /**
     Asynchronously add objects to the database

     - parameter objects:    objects to be added to the database
     - parameter update:     indicates whether the record should be updated if already present
     */

    public func asyncAddObjects <S: Storable> (objects: [S], update: Bool = true, withCompletionHandler completionHandler: ((Result<Bool>)->Void)? = nil) {
       queue.async() { [weak self] () -> Void in
            guard self != nil else {
                return
            }

            completionHandler?(self!.addObjects(objects: objects))
        }
    }

    /**
     Asynchronous retrieval of data for a specified type, matching a filter, from the database

     - parameter filters:   dictionary containing the filters identifying objects to be retrieved
     - parameter type:      type of the objects to be retrieved
    */

    public func asyncDataForType <S: Storable> (type: S.Type, matchingFilter filter: Filter? = nil, withCompletionHandler completionHandler: @escaping ((Result<[[String: Value?]]>)->Void)) {
        queue.async() { [weak self] () -> Void in
            guard self != nil else {
                return
            }

            completionHandler(self!.dataForType(type: type, matchingFilter: filter))
        }
    }

    /**
     Asynchronously remove objects of a specified type, matching a filter, from the database

     - parameter filters:   dictionary containing the filters identifying objects to be deleted
     - parameter type:      type of the objects to be deleted
     */

    public func asyncDeleteObjectsForType (type: Storable.Type, matchingFilter filter: Filter? = nil, withCompletionHandler completionHandler: ((Result<Bool>)->Void)? = nil) {
        queue.async() { [weak self] () -> Void in
            guard self != nil else {
                return
            }

            completionHandler?(self!.deleteObjectsForType(type: type, matchingFilter: filter))
        }
    }
}

extension SwiftyDB {

// MARK: - Asynchronous dynamic initialization

    /**
     Asynchronous retrieval of objects of a specified type, matching a set of filters, from the database

     - parameter filters:   dictionary containing the filters identifying objects to be retrieved
     - parameter type:      type of the objects to be retrieved
    */

    public func asyncObjectsForType <D : Storable> (type: D.Type, matchingFilter filter: Filter? = nil, withCompletionHandler completionHandler: @escaping ((Result<[D]>)->Void)) where D : NSObject {
         self.queue.async() { [unowned self] () -> Void in
            completionHandler(self.objectsForType(type: type, matchingFilter: filter))
        }
    }
}
