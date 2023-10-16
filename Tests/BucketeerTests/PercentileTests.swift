import XCTest
@testable import Bucketeer

final class PercentileTests: XCTestCase {
  
  func testPercentileBuckets() {
    let collector = Bucketeer<MemoryDataSet>(dataSet: .init(items: []))
    
    collector.dataSet.items = []
    let empty = collector.buckets(by: .single, option: .percentiles([0.75, 0.9]))
    XCTAssertEqual(empty.count, 2)
    XCTAssertEqual(empty.map(\.count), [0, 0])

    collector.dataSet.items = [nil, nil, nil]
    let nils = collector.buckets(by: .single, option: .percentiles([0.75, 0.9]))
    XCTAssertEqual(nils.count, 2)
    XCTAssertEqual(nils.map(\.count), [0, 0])

    collector.dataSet.items = Array(repeating: 1.0, count: 1000)
    let uniforms = collector.buckets(by: .single, option: .percentiles([0.75, 0.9]))
    XCTAssertEqual(uniforms.count, 3)
    XCTAssertEqual(uniforms.map(\.count), [0, 0, 1000])

    collector.dataSet.items = Array(repeating: 1.0, count: 1000) + Array(repeating: 2.0, count: 1000)
    let doubles = collector.buckets(by: .single, option: .percentiles([0.75, 0.9]))
    XCTAssertEqual(doubles.count, 3)
    XCTAssertEqual(doubles.map(\.count), [1000, 0, 1000])

    collector.dataSet.items = Array(stride(from: 5, to: 5.5, by: 0.001))
    let strode = collector.buckets(by: .single, option: .percentiles([0.75, 0.9]))
    XCTAssertEqual(strode.count, 3)
    XCTAssertEqual(strode.map(\.count), [375, 75, 50])

    collector.dataSet.items = Array(stride(from: 0, to: 10, by: 0.01)) + [nil, nil, nil]
    let strodeWithNils = collector.buckets(by: .single, option: .percentiles([0.75, 0.9]))
    XCTAssertEqual(strodeWithNils.count, 3)
    XCTAssertEqual(strodeWithNils.map(\.count), [750, 150, 100])
  }
  
  func testPercentileBucketsFromTooFewItems() {
    let collector = Bucketeer<MemoryDataSet>(dataSet: .init(items: []))
    
    collector.dataSet.items = [1]
    let single = collector.buckets(by: .single, option: .percentiles([0.75, 0.9]))
    XCTAssertEqual(single.count, 3)
    XCTAssertEqual(single.map(\.count), [0, 0, 1])

    collector.dataSet.items = [1, 1]
    let double = collector.buckets(by: .single, option: .percentiles([0.75, 0.9]))
    XCTAssertEqual(double.count, 3)
    XCTAssertEqual(double.map(\.count), [0, 0, 2])

    collector.dataSet.items = [1, 1, 1]
    let triple = collector.buckets(by: .single, option: .percentiles([0.75, 0.9]))
    XCTAssertEqual(triple.count, 3)
    XCTAssertEqual(triple.map(\.count), [0, 0, 3])

    collector.dataSet.items = [1, 1, 1, 1]
    let quadruple = collector.buckets(by: .single, option: .percentiles([0.75, 0.9]))
    XCTAssertEqual(quadruple.count, 3)
    XCTAssertEqual(quadruple.map(\.count), [0, 0, 4])
  }
  
  func testPercentilesFromInfiniteBuckets() {
    let collector = Bucketeer<MemoryDataSet>(dataSet: .init(items: []))
    
    collector.dataSet.items = Array(repeating: .infinity * -1, count: 91) + [-139872, -1625, -961]
    let uniforms = collector.buckets(by: .single, option: .percentiles([0.75, 0.9]))
    XCTAssertEqual(uniforms.count, 3)
    XCTAssertEqual(uniforms.map(\.count), [91, 0, 3])
  }
}

// MARK: - Test data

struct MemoryDataSet: BucketeerDataSet {
  enum Metric: Hashable {
    case single
  }
  
  var items: [Double?]
  
  func value(for item: Double?, metric: Metric) -> Double? {
    return item
  }
}
