import Foundation

public protocol BucketeerDataSet {
  associatedtype Item
  associatedtype Metric: Hashable
  
  var items: [Item] { get }
  
  func value(for item: Item, metric: Metric) -> Double?
}

/// A one-dimensional data set of numbers
public struct NumericDataSet: BucketeerDataSet {
  public init(items: [Double]) {
    self.items = items
  }
  
  public enum Metric: Hashable {
    case value
  }
  
  public var items: [Double]
  public func value(for item: Double, metric: Metric) -> Double? { item }
}

public struct Bucket: Hashable {
  public enum Kind: Hashable {
    case fixed(threshold: Double)
    case percentile(Double)
    case uniform(index: Int)
  }
  
  public let kind: Kind
  var range: Range<Double>? = nil
  public var count: Int = 0
  
  public func contains(_ value: Double) -> Bool {
    guard let range else { return false }
    if range.contains(value) {
      return true
    } else if value == .infinity, range.upperBound == .infinity {
      return true
    } else {
      return false
    }
  }
}

public enum BucketOption {
  /// Creates `thresholds.count` buckets with the provided cut-off values
  case fixed(thresholds: [Double])

  /// Creates `count` + 1 buckets at the provided percentiles (i.e., values
  /// have to be between 0...1.
  case percentiles([Double])

  /// Creates `n` buckets of uniform width from min to max
  case uniform(Int)
}

public class Bucketeer<DataSet> where DataSet: BucketeerDataSet {
  public struct Analysis {
    let values: [Double]
  }
  
  /// Prepares for a new data set
  ///
  /// - warning: Whenever the data set's `items` change, you could either create a new
  /// `Bucketeer` instance or call `clearCache()`
  ///
  /// - Parameter dataSet: The data set to analyse
  public init(dataSet: DataSet) {
    self.dataSet = dataSet
  }
  
  public var dataSet: DataSet { didSet { cachedValues = [:] }}
  
  private var cachedValues: [DataSet.Metric: [Double]] = [:]
  
  private func values(for metric: DataSet.Metric) -> [Double] {
    if let cached = cachedValues[metric] {
      return cached
    } else {
      let values = dataSet.items.compactMap { dataSet.value(for: $0, metric: metric) }.sorted()
      cachedValues[metric] = values
      return values
    }
  }
  
  /// Clears the cache. Important to call this whenever `items` of your data set changes.
  public func clearCache() {
    cachedValues = [:]
  }
  
  public func analyze(_ metric: DataSet.Metric) -> Analysis {
    let values = self.values(for: metric)
    return Analysis(values: values)
  }
  
  /// Puts the data set's items into histogram-compatible buckets for the provided metric
  ///
  /// - Parameters:
  ///   - metric: The metric by which to put items into buckets
  ///   - option: Determines how to split the items into buckets, e.g., by provided thresholds, by equal "width", or according to percentiles. See `BucketOption`.
  /// - Returns: The resulting buckets
  public func buckets(by metric: DataSet.Metric, option: BucketOption) -> [Bucket] {
    let values = self.values(for: metric)

    var buckets: [Bucket]
    switch option {
    case .uniform(let bucketCount):
      guard let min = values.first, let max = values.last else {
        // No values, but let's maintain requested number of buckets
        return (0..<bucketCount).map { .init(kind: .uniform(index: $0)) }
      }

      if bucketCount <= 1 {
        // Trivial case for single bucket
        buckets = [.init(kind: .uniform(index: 0), range: (-.infinity)..<(.infinity))]
      } else if min == max {
        // Special handling with min=max to avoid duplicated buckets
        buckets = (0..<bucketCount).map { i -> Bucket in
          let lower = (i == bucketCount / 2) ? min : min - Double(bucketCount / 2 - i)
          let upper = lower + 1
          return .init(kind: .uniform(index: i), range: lower..<upper)
        }
      } else {
        // Regular case, first and last bucket but use infinite at the sides
        // to address floating point imprecision
        let width = (max - min) / Double(bucketCount)
        buckets = (0..<bucketCount).map { i -> Bucket in
          let lower = min + Double(i) * width
          let upper = lower + width
          let range: Range<Double>
          switch i {
          case ...0:                  range = -.infinity..<upper
          case ..<(bucketCount - 1):  range = lower..<upper
          default:                    range = lower..<(.infinity)
          }
          return .init(kind: .uniform(index: i), range: range)
        }
      }
      
    case .percentiles(var percentiles):
      guard !values.isEmpty else {
        return percentiles.map { .init(kind: .percentile($0)) }
      }
      
      if percentiles.last == 1.0 {
        percentiles.removeLast()
      }
      let splits = Set(percentiles.map { Int(Double(values.count) * $0) })
      
      let thresholds = splits
        .map { values[$0] }
        .sorted()
      
      percentiles.append(1.0)
      let bucketCount = percentiles.count
      
      // If we have more buckets/percentiles than items, we just stick
      // everything in the last bucket.
      guard bucketCount <= thresholds.count + 1 else {
        return percentiles.enumerated().map { i, percentile in
          if i == bucketCount - 1 {
            return .init(kind: .percentile(percentile), range: -.infinity ..< .infinity, count: values.count)
          } else {
            return .init(kind: .percentile(percentile))
          }
        }
      }
      
      buckets = percentiles.enumerated().map { i, percentile in
        let range: Range<Double>
        switch i {
        case ...0:                  range = -.infinity..<thresholds[0]
        case ..<(bucketCount - 1):  range = thresholds[i-1]..<thresholds[i]
        default:                    range = thresholds[i-1]..<(.infinity)
        }
        return .init(kind: .percentile(percentile), range: range)
      }
    
    case .fixed(var thresholds):
      if thresholds.last != .infinity {
        thresholds.append(.infinity)
      }
      
      let bucketCount = thresholds.count
      buckets = thresholds.enumerated().map { i, upper in
        let range: Range<Double>
        switch i {
        case ...0:                  range = -.infinity..<thresholds[0]
        case ..<(bucketCount - 1):  range = thresholds[i-1]..<thresholds[i]
        default:                    range = thresholds[i-1]..<(.infinity)
        }
        return .init(kind: .fixed(threshold: upper), range: range)
      }
    }
    
    for value in values {
      var anyMatched: Bool = false
      for (offset, bucket) in buckets.enumerated() {
        if bucket.contains(value) {
          buckets[offset].count += 1
          anyMatched = true
        }
      }
      
      if !anyMatched {
        assert(value == .infinity)
        buckets[buckets.count - 1].count += 1
      }
    }

    return buckets.sorted()
  }
}

extension Bucket: Comparable {
  public static func < (lhs: Bucket, rhs: Bucket) -> Bool {
    guard let leftRange = lhs.range, let rightRange = rhs.range else { return false }
    return leftRange.lowerBound < rightRange.lowerBound
  }
}
