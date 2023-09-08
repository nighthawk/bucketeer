import XCTest
@testable import Bucketeer

final class CodableTests: XCTestCase {
  
  func testRoundTripFixedBucket() throws {
    let collector = Bucketeer(dataSet: TripsDataSet.trips)
    
    let buckets = collector.buckets(by: .distance, option: .fixed(thresholds: [
      1_000, 2_500, 5_000, 10_000
    ]))
    XCTAssertEqual(buckets.count, 5)
    XCTAssertEqual(buckets.map(\.count), [1, 2, 1, 2, 1])
    
    let encoded = try JSONEncoder().encode(buckets)
    let restored = try JSONDecoder().decode([Bucket].self, from: encoded)
    XCTAssertEqual(buckets, restored)
  }
  
  func testRoundTripUniformBucket() throws {
    let collector = Bucketeer(dataSet: TripsDataSet.trips)
    
    let single = collector.buckets(by: .distance, option: .uniform(1))
    XCTAssertEqual(single.count, 1)
    XCTAssertEqual(single.map(\.count), [7])

    let singleE = try JSONEncoder().encode(single)
    let singleD = try JSONDecoder().decode([Bucket].self, from: singleE)
    XCTAssertEqual(single, singleD)

    let triple = collector.buckets(by: .distance, option: .uniform(3))
    XCTAssertEqual(triple.count, 3)
    XCTAssertEqual(triple.map(\.count), [4, 2, 1])

    let tripleE = try JSONEncoder().encode(triple)
    let tripleD = try JSONDecoder().decode([Bucket].self, from: tripleE)
    XCTAssertEqual(triple, tripleD)

    let quintuple = collector.buckets(by: .distance, option: .uniform(5))
    XCTAssertEqual(quintuple.count, 5)
    XCTAssertEqual(quintuple.map(\.count), [3, 1, 1, 1, 1])
    
    let quintupleE = try JSONEncoder().encode(quintuple)
    let quintupleD = try JSONDecoder().decode([Bucket].self, from: quintupleE)
    XCTAssertEqual(quintuple, quintupleD)
  }
  
  func testRoundTripPercentileBucket() throws {
    let collector = Bucketeer(dataSet: TripsDataSet.trips)
    
    let single = collector.buckets(by: .distance, option: .percentiles([0.5]))
    XCTAssertEqual(single.count, 2)
    XCTAssertEqual(single.map(\.count), [3, 4])

    let singleE = try JSONEncoder().encode(single)
    let singleD = try JSONDecoder().decode([Bucket].self, from: singleE)
    XCTAssertEqual(single, singleD)

    let triple = collector.buckets(by: .distance, option: .percentiles([0.3, 0.7]))
    XCTAssertEqual(triple.count, 3)
    XCTAssertEqual(triple.map(\.count), [1, 3, 3])
    
    let tripleE = try JSONEncoder().encode(triple)
    let tripleD = try JSONDecoder().decode([Bucket].self, from: tripleE)
    XCTAssertEqual(triple, tripleD)

    let quintuple = collector.buckets(by: .distance, option: .percentiles([0.1, 0.3, 0.7, 0.9]))
    XCTAssertEqual(quintuple.count, 5)
    XCTAssertEqual(quintuple.map(\.count), [0, 1, 3, 2, 1])
    
    let quintupleE = try JSONEncoder().encode(quintuple)
    let quintupleD = try JSONDecoder().decode([Bucket].self, from: quintupleE)
    XCTAssertEqual(quintuple, quintupleD)
  }
  
}
