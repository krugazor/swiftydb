import Foundation
import XCTest

@testable import SwiftyDB

@objc(TestData)
class TestData : NSObject, Storable {
    var objectid : NSNumber?
    var str : String?
    // var date : NSDate?
    
    override required init() {
    }
    
    var accessInstanceVariablesDirectly: Bool {
        get { return true }
    }
}

extension TestData : PrimaryKeys {
    class func primaryKeys() -> Set<String> {
        return ["objectid"]
    }
}

class DBTests : XCTestCase {
    static let dbtestpath = "/tmp/dbtests-110.sqlite"
    static let dbtestname = "dbtests-110"
    
    private var database : SwiftyDB?
    
    /*!
     * @method +setUp
     * Setup method called before the invocation of any test method in the class.
     */
    public override func setUp() {
        // because xctest has a tendancy to crash
        do {
            try FileManager.default.removeItem(atPath: DBTests.dbtestpath)
        } catch {
        }
        database = SwiftyDB(path: DBTests.dbtestpath)
    }
    
    
    /*!
     * @method +testDown
     * Teardown method called after the invocation of every test method in the class.
     */
    public override func tearDown() {
        defer {            
            database = nil
        }
    }
    
    func testDatabase() {
        let input = TestData()
        input.objectid = 183
        input.str = "this is a test"
        // input.date = NSDate()
        
        input.setValuesForKeys([:])
        
        
        let addres = database?.addObject(object: input)
        XCTAssertNil(addres?.error, (addres?.error!.localizedDescription)!)
        
        let result = database?.objectsForType(type: TestData.self)
        
        XCTAssertNil(result?.error, (result?.error!.localizedDescription)!)
        XCTAssert(result?.value!.count == 1, "not matching the data")
        
        if(result?.error != nil || (result?.value!.count)! > 0) {
            let output = result?.value?[0]
            XCTAssert(input.objectid == output!.objectid)
            XCTAssert(input.str == output!.str)
            // XCTAssert(input.date == output?.date)
        }
        
        
    }
    
    func testLimitsAndOffsets() {
        for i in 1...100 {
            let input = TestData()
            input.objectid = NSNumber(value:i)
            input.str = "this is a test"
            
            let r = database?.addObject(object: input)
            XCTAssertNil(r?.error, (r?.error?.localizedDescription)!)
        }
        
        let tenResults = database?.objectsForType(type: TestData.self, limit: 10, orderBy: "objectid")
        XCTAssertNil(tenResults?.error, (tenResults?.error!.localizedDescription)!)
        XCTAssert(tenResults?.value?.count == 10, "not matching the data")
        guard tenResults?.value?.count == 10 else {
            return
        }
        
        XCTAssertLessThan((tenResults?.value?[0].objectid?.intValue)!, (tenResults?.value?[9].objectid?.intValue)!, "data is not ordered")
        
        let twentyOffsetted = database?.objectsForType(type: TestData.self, limit: 20, offset: 10, orderBy: "objectid")
        XCTAssertNil(twentyOffsetted?.error, (twentyOffsetted?.error!.localizedDescription)!)
        XCTAssert(twentyOffsetted?.value!.count == 20, "not matching the data")
        guard twentyOffsetted?.value!.count == 20 else {
            return
        }
        
        XCTAssertEqual(twentyOffsetted?.value![0].objectid!.intValue, 11, "expected 11, got " + String(describing: twentyOffsetted?.value![0].objectid!.intValue))
    }
}
