import XCTest
@testable import Bucketeer

final class TripsTests: XCTestCase {
  
  func testTripValues() {
    let collector = Bucketeer(dataSet: TripsDataSet.trips)
    
    let durations = collector.analyze(.duration).values
    XCTAssertEqual(durations, [10.0 * 60, 15.0 * 60, 16.0 * 60, 31.0 * 60, 38.0 * 60, 62.0 * 60, 2.5 * 3600])

    let distances = collector.analyze(.distance).values
    XCTAssertEqual(distances, [800.0, 1000.0, 1000.0, 4000.0, 7500.0, 7800.0, 12000.0])
  }

  func testTripFixedBuckets() {
    let collector = Bucketeer(dataSet: TripsDataSet.trips)
    
    let durations = collector.buckets(by: .distance, option: .fixed(thresholds: [
      1_000, 2_500, 5_000, 10_000
    ]))
    XCTAssertEqual(durations.count, 5)
    XCTAssertEqual(durations.map(\.count), [1, 2, 1, 2, 1])
  }
  
  func testTripUniformBuckets() {
    let collector = Bucketeer(dataSet: TripsDataSet.trips)
    
    let single = collector.buckets(by: .distance, option: .uniform(1))
    XCTAssertEqual(single.count, 1)
    XCTAssertEqual(single.map(\.count), [7])

    let triple = collector.buckets(by: .distance, option: .uniform(3))
    XCTAssertEqual(triple.count, 3)
    XCTAssertEqual(triple.map(\.count), [4, 2, 1])
    
    let quintuple = collector.buckets(by: .distance, option: .uniform(5))
    XCTAssertEqual(quintuple.count, 5)
    XCTAssertEqual(quintuple.map(\.count), [3, 1, 1, 1, 1])
  }
  
  func testTripPercentileBuckets() {
    let collector = Bucketeer(dataSet: TripsDataSet.trips)
    
    let single = collector.buckets(by: .distance, option: .percentiles([0.5]))
    XCTAssertEqual(single.count, 2)
    XCTAssertEqual(single.map(\.count), [3, 4])

    let triple = collector.buckets(by: .distance, option: .percentiles([0.3, 0.7]))
    XCTAssertEqual(triple.count, 3)
    XCTAssertEqual(triple.map(\.count), [1, 3, 3])
    
    let quintuple = collector.buckets(by: .distance, option: .percentiles([0.1, 0.3, 0.7, 0.9]))
    XCTAssertEqual(quintuple.count, 5)
    XCTAssertEqual(quintuple.map(\.count), [0, 1, 3, 2, 1])
  }
  
  func testEmptyDataSet() {
    let collector = Bucketeer(dataSet: TripsDataSet(items: []))
    
    let durations = collector.analyze(.duration).values
    XCTAssertEqual(durations, [])

    let distances = collector.analyze(.distance).values
    XCTAssertEqual(distances, [])
    
    let fixed = collector.buckets(by: .distance, option: .fixed(thresholds: [
      1_000, 2_500, 5_000, 10_000
    ]))
    XCTAssertEqual(fixed.count, 5)
    XCTAssertEqual(fixed.map(\.count), [0, 0, 0, 0, 0])
    
    let single = collector.buckets(by: .distance, option: .uniform(1))
    XCTAssertEqual(single.count, 1)
    XCTAssertEqual(single.map(\.count), [0])

    let triple = collector.buckets(by: .distance, option: .uniform(3))
    XCTAssertEqual(triple.count, 3)
    XCTAssertEqual(triple.map(\.count), [0, 0, 0])
  }
  
  func testAllTheSame() {
    let items = (0..<100).map { _ in TripItem(duration: .init(value: 1, unit: .hours), distance: .init(value: 10, unit: .kilometers)) }
    let collector = Bucketeer(dataSet: TripsDataSet(items: items))
    
    let durations = collector.analyze(.duration).values
    XCTAssertEqual(durations, (0..<100).map { _ in 3600.0 })

    let distances = collector.analyze(.distance).values
    XCTAssertEqual(distances, (0..<100).map { _ in 10_000.0 })
    
    let fixed = collector.buckets(by: .distance, option: .fixed(thresholds: [
      1_000, 2_500, 5_000, 10_000
    ]))
    XCTAssertEqual(fixed.count, 5)
    XCTAssertEqual(fixed.map(\.count), [0, 0, 0, 0, 100])
    
    let single = collector.buckets(by: .distance, option: .uniform(1))
    XCTAssertEqual(single.count, 1)
    XCTAssertEqual(single.map(\.count), [100])
    
    let double = collector.buckets(by: .distance, option: .uniform(2))
    XCTAssertEqual(double.count, 2)
    XCTAssertEqual(double.map(\.count), [0, 100])

    let triple = collector.buckets(by: .distance, option: .uniform(3))
    XCTAssertEqual(triple.count, 3)
    XCTAssertEqual(triple.map(\.count), [0, 100, 0])
  }
  
  func testMissingValues() {
    let collector = Bucketeer(dataSet: TripsDataSet(items: [
      .init(duration: .init(value:  3, unit: .hours)),
      .init(duration: .init(value: 10, unit: .minutes)),
      .init(duration: .init(value: -0, unit: .hours)),
      .init(duration: .init(value: 24, unit: .hours)),
    ]))
    
    let triple = collector.buckets(by: .distance, option: .uniform(3))
    XCTAssertEqual(triple.count, 3)
    XCTAssertEqual(triple.map(\.count), [0, 0, 0])  }
  
  func testNegativeValues() {
    let collector = Bucketeer(dataSet: TripsDataSet(items: [
      .init(duration: .init(value: -2, unit: .hours)),
      .init(duration: .init(value: -1, unit: .hours)),
      .init(duration: .init(value: -0, unit: .hours)),
      .init(duration: .init(value: 24, unit: .hours)),
    ]))
    
    let triple = collector.buckets(by: .duration, option: .uniform(3))
    XCTAssertEqual(triple.count, 3)
    XCTAssertEqual(triple.map(\.count), [3, 0, 1])
  }
  
  func testPerformanceCaching() {
    let items = (0..<100_000).map { _ in TripItem(duration: .init(value: Double.random(in: 10..<120), unit: .minutes), distance: .init(value: Double.random(in: 500..<12_500), unit: .meters)) }
    let collector = Bucketeer(dataSet: TripsDataSet(items: items))

    measure {
      _ = collector.buckets(by: .distance, option: .percentiles([0.5, 0.8, 0.9]))
      _ = collector.buckets(by: .duration, option: .percentiles([0.25, 0.5, 0.8, 0.9]))
    }
    
  }

}

// MARK: - Test data

extension BucketeerDataSet {
  static var trips: TripsDataSet { TripsDataSet(
    items: [
      .init(duration: .init(value: 31, unit: .minutes),
            distance: .init(value: 7.5, unit: .kilometers)),
      .init(duration: .init(value: 38, unit: .minutes),
            distance: .init(value: 7.8, unit: .kilometers)),
      .init(duration: .init(value: 62, unit: .minutes),
            distance: .init(value: 12, unit: .kilometers)),
      .init(duration: .init(value: 2.5, unit: .hours),
            distance: .init(value: 4, unit: .kilometers)),
      .init(duration: .init(value: 15, unit: .minutes),
            distance: .init(value: 1, unit: .kilometers)),
      .init(duration: .init(value: 16, unit: .minutes),
            distance: .init(value: 1, unit: .kilometers)),
      .init(duration: .init(value: 10, unit: .minutes),
            distance: .init(value: 800, unit: .meters)),
    ])
  }
}

struct TripItem {
  let duration: Measurement<UnitDuration>
  var distance: Measurement<UnitLength>? = nil
}

struct TripsDataSet: BucketeerDataSet {
  enum Metric: Hashable {
    case duration
    case distance
  }
  
  var items: [TripItem]
  
  func value(for item: TripItem, metric: Metric) -> Double? {
    switch metric {
    case .distance: return item.distance?.converted(to: .meters).value
    case .duration: return item.duration.converted(to: .seconds).value
    }
  }
}
