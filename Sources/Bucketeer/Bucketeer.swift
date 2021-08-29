import Foundation

public protocol BucketeerDataSet {
  associatedtype Item
  associatedtype Metric
  
  var items: [Item] { get }
  
  func value(for item: Item, metric: Metric) -> Double?
}

public class Bucketeer<Data> where Data: BucketeerDataSet {
  public struct Analysis {
    let values: [Double]
  }
  
  public struct Bucket {
    public let range: Range<Double>
    public var count: Int = 0
  }
  
  public enum BucketOption {
    // Creates `thresholds.count` buckets with the provided cut-off values
    case fixed(thresholds: [Double])

    // Creates `count` buckets at the provided percentiles (i.e., values
    // have to be between 0...1.
    case percentiles([Double])

    // Creates `n` buckets of uniform width from min to max
    case uniform(Int)
  }
  
  public init(data: Data) {
    self.data = data
  }
  
  public var data: Data
  
  private func values(for metric: Data.Metric) -> [Double] {
    #warning("TODO: Use cache")
    let values = data.items.compactMap { data.value(for: $0, metric: metric) }.sorted()
    return values
  }
  
  public func analyze(_ metric: Data.Metric) -> Analysis {
    let values = self.values(for: metric)
    return Analysis(values: values)
  }
  
  public func buckets(by metric: Data.Metric, option: BucketOption) -> [Bucket] {
    let values = self.values(for: metric)

    var ranges: [Range<Double>]
    switch option {
    case .uniform(let bucketCount):
      guard let min = values.first, let max = values.last else {
        // No values, but let's maintain requested number of buckets
        return (0..<bucketCount).map { _ in .init(range: (-.infinity)..<(.infinity)) }
      }
      if bucketCount <= 1 {
        // Trivial case for single bucket
        ranges = [(-.infinity)..<(.infinity)]
      } else if min == max {
        // Special handling with min=max to avoid duplicated buckets
        ranges = (0..<bucketCount).map { i -> Range<Double> in
          let lower = (i == bucketCount / 2) ? min : min - Double(bucketCount / 2 - i)
          let upper = lower + 1
          return lower..<upper
        }
      } else {
        // Regular case, first and last bucket but use infinite at the sides
        // to address floating point imprecision
        let width = (max - min) / Double(bucketCount)
        ranges = (0..<bucketCount).map { i -> Range<Double> in
          let lower = min + Double(i) * width
          let upper = lower + width
          switch i {
          case ...0:                  return -.infinity..<upper
          case ..<(bucketCount - 1):  return lower..<upper
          default:                    return lower..<(.infinity)
          }
        }
      }
      
    case .percentiles(let percentiles):
      guard !values.isEmpty else { return [] }
      var splits = Set(percentiles.map { Int(Double(values.count) * $0) })
      
      // Make sure we hve the bounds, but don't double up with them
      splits.formUnion([0, values.count - 1])
      
      #warning("TODO: Should maintain number of buckets here, too, even if there's nothing unique in that percentile... should it go up or down?")
      let values = splits.map { values[$0] }
      var sorted = values.sorted()
      sorted[0] = -.infinity
      sorted[sorted.count - 1] = .infinity
      ranges = zip(sorted.dropLast(), sorted.dropFirst()).map { $0..<$1 }
    
    case .fixed(var thresholds):
      thresholds.insert(-.infinity, at: 0)
      thresholds.append(.infinity)
      ranges = zip(thresholds.dropLast(), thresholds.dropFirst()).map { $0..<$1 }
    }
    
    var bucketsByRange: [Range<Double>: Bucket] = Dictionary(uniqueKeysWithValues: ranges.map {
      ($0, Bucket(range: $0))
    })
    for value in values {
      if let range = ranges.first(where: { $0.contains(value) } ){
        bucketsByRange[range, default: .init(range: range)].count += 1
      }
    }
    assert(bucketsByRange.values.map(\.count).reduce( 0, +) == values.count)
    return bucketsByRange.values.sorted()
  }
}

extension Bucketeer.Bucket: Comparable {
  public static func < (lhs: Bucketeer<Data>.Bucket, rhs: Bucketeer<Data>.Bucket) -> Bool {
    lhs.range.lowerBound < rhs.range.lowerBound
  }
}
