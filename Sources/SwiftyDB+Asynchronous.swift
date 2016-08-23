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
    #if os(Linux)
    private var queue: dispatch_queue_t {
        return dispatch_get_global_queue(.QOS_CLASS_USER_INITIATED)
    }
    #else
    private var queue: DispatchQueue {
        return DispatchQueue.global(qos: .userInitiated)
    }
    #endif

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
      #if os(Linux)
      dispatch_barrier_async(queue, {
        [weak self] () -> Void in
              guard self != nil else {
                  return
              }

              completionHandler?(self!.addObjects(objects: objects))
      })
      #else
      queue.async() { [weak self] () -> Void in
            guard self != nil else {
                return
            }

            completionHandler?(self!.addObjects(objects: objects))
        }
      #endif
    }

    /**
     Asynchronous retrieval of data for a specified type, matching a filter, from the database

     - parameter filters:   dictionary containing the filters identifying objects to be retrieved
     - parameter type:      type of the objects to be retrieved
    */

    public func asyncDataForType <S: Storable> (type: S.Type, matchingFilter filter: Filter? = nil, withCompletionHandler completionHandler: ((Result<[[String: Value?]]>)->Void)) {
      #if os(Linux)
      dispatch_barrier_async(queue, {
        [weak self] () -> Void in
              guard self != nil else {
                  return
              }

              completionHandler?(self!.dataForType(type: type, matchingFilter: filter))
      })
      #else
        queue.async() { [weak self] () -> Void in
            guard self != nil else {
                return
            }

            completionHandler(self!.dataForType(type: type, matchingFilter: filter))
        }
      #endif
    }

    /**
     Asynchronously remove objects of a specified type, matching a filter, from the database

     - parameter filters:   dictionary containing the filters identifying objects to be deleted
     - parameter type:      type of the objects to be deleted
     */

    public func asyncDeleteObjectsForType (type: Storable.Type, matchingFilter filter: Filter? = nil, withCompletionHandler completionHandler: ((Result<Bool>)->Void)? = nil) {
      #if os(Linux)
      dispatch_barrier_async(queue, {
        [weak self] () -> Void in
              guard self != nil else {
                  return
              }

              completionHandler?(self!.deleteObjectsForType(type: type, matchingFilter: filter))
      })
      #else
        queue.async() { [weak self] () -> Void in
            guard self != nil else {
                return
            }

            completionHandler?(self!.deleteObjectsForType(type: type, matchingFilter: filter))
        }
      #endif
    }
}

extension SwiftyDB {

// MARK: - Asynchronous dynamic initialization

    /**
     Asynchronous retrieval of objects of a specified type, matching a set of filters, from the database

     - parameter filters:   dictionary containing the filters identifying objects to be retrieved
     - parameter type:      type of the objects to be retrieved
    */

    public func asyncObjectsForType <D where D: Storable, D: NSObject> (type: D.Type, matchingFilter filter: Filter? = nil, withCompletionHandler completionHandler: ((Result<[D]>)->Void)) {
      #if os(Linux)
      dispatch_barrier_async(queue, {
        [unowned self] () -> Void in
            completionHandler(self.objectsForType(type: type, matchingFilter: filter))
      })
      #else
        queue.async() { [unowned self] () -> Void in
            completionHandler(self.objectsForType(type: type, matchingFilter: filter))
        }
      #endif
    }
}
