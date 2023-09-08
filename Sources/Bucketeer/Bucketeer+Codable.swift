//
//  Bucketeer+Codable.swift
//
//
//  Created by Adrian Schönig on 8/9/2023.
//

import Foundation

extension Bucket.Kind: Codable {
  enum CodingKeys: String, CodingKey {
    case type
    case threshold
    case percentile
    case index
  }
  
  public func encode(to encoder: Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    switch self {
    case .fixed(let threshold):
      try container.encode("fixed", forKey: .type)
      if threshold != .infinity {
        try container.encode(threshold, forKey: .threshold)
      }
    case .percentile(let percentile):
      try container.encode("percentile", forKey: .type)
      try container.encode(percentile, forKey: .percentile)
    case .uniform(let index):
      try container.encode("uniform", forKey: .type)
      try container.encode(index, forKey: .index)
    }
  }
  
  public init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let type = try container.decode(String.self, forKey: .type)
    switch type {
    case "fixed":
      self = .fixed(
        threshold: try container.decodeIfPresent(Double.self, forKey: .threshold) ?? .infinity
      )
    case "percentile":
      self = .percentile(
        try container.decode(Double.self, forKey: .percentile)
      )
    case "uniform":
      self = .uniform(
        index: try container.decode(Int.self, forKey: .index)
      )
    default:
      throw DecodingError.dataCorrupted(.init(
        codingPath: decoder.codingPath,
        debugDescription: "Invalid type: \(type)"
      ))
    }
  }
}

extension Bucket: Codable {
  enum CodingKeys: String, CodingKey {
    case kind
    case min
    case max
    case hasRange
    case count
  }
  
  public func encode(to encoder: Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    try container.encode(kind, forKey: .kind)
    try container.encode(count, forKey: .count)
    if let range {
      try container.encode(true, forKey: .hasRange)
      if range.lowerBound != -.infinity {
        assert(range.lowerBound != .infinity)
        try container.encode(range.lowerBound, forKey: .min)
      }
      if range.upperBound != .infinity {
        assert(range.upperBound != -.infinity)
        try container.encode(range.upperBound, forKey: .max)
      }
    } else {
      try container.encode(false, forKey: .hasRange)
    }
  }
  
  public init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    self.kind = try container.decode(Kind.self, forKey: .kind)
    self.count = try container.decode(Int.self, forKey: .count)
    if try container.decode(Bool.self, forKey: .hasRange) {
      let min = try container.decodeIfPresent(Double.self, forKey: .min) ?? -.infinity
      let max = try container.decodeIfPresent(Double.self, forKey: .max) ?? .infinity
      self.range = min ..< max
    } else {
      self.range = nil
    }
  }
}
