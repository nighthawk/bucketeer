# Bucketeer

A Swift mini-framework for analysing data, where you have a data set of different items, each item can have different metrics attached to it, and you want to put those into different buckets.

Feature set:

- `Bucketeer.buckets(by:option)`: Split data into buckets by fixed thresholds for each bucket, by having buckets of uniform width, or by buckets of the provided percentiles. 
- Those buckets are `Codable`
