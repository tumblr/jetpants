#!/usr/bin/env ruby

# This is a script to count all rows in your database topology. It
# performs concurrent chunked counts on standby slaves, and can serve
# as an example of writing scripts using Jetpants as a library.

require 'jetpants'

# Count on shards first
sharded_tables = Jetpants::Table.from_config 'sharded_tables'
sharded_counts = Jetpants.shards.limited_concurrent_map(8) do |p|
  node = p.standby_slaves.last
  node.stop_query_killer
  node.output "Starting counts for #{p}"
  my_max = p.max_id
  if my_max == 'INFINITY'
    maxes = sharded_tables.map do |t|
      node.query_return_first_value "SELECT MAX(#{t.sharding_keys.first}) FROM #{t.name}"
    end
    my_max = maxes.map(&:to_i).max
  end
  counts = node.row_counts(sharded_tables, p.min_id, my_max)
  p.output "Found counts for #{p}: #{counts}"
  node.start_query_killer
  counts
end

total_counts = {'rollup' => 0}
sharded_counts.each do |cnt|
  cnt.each do |table_name, row_count|
    total_counts[table_name] ||= 0
    total_counts[table_name] += row_count
    total_counts['rollup'] += row_count
  end
end

# Count on functional partitions next
global_counts = Jetpants.functional_partitions.limited_concurrent_map(8) do |p|
  begin
    node = p.standby_slaves.last
    node.stop_query_killer
    node.output "Starting counts for #{p}"
    counts = {}
    p.tables.each do |t|
      min, max = false, false
      if t.first_pk_col
        vals = node.query_return_first "SELECT MIN(#{t.first_pk_col}) AS minval, MAX(#{t.first_pk_col}) AS maxval FROM #{t.name}"
        min, max = vals[:minval], vals[:maxval]
        if max.to_i - min.to_i > 100
          t.chunks = 100
        else
          min, max = false, false
        end
      end
      counts.merge! node.row_counts(t, min, max)
    end
    p.output "Found counts for #{p}: #{counts}"
    node.start_query_killer
    counts
  rescue => ex
    p.output "Unable to obtain counts for #{p} -- #{ex.message}"
    node.start_query_killer
    {}
  end
end

global_counts.each do |cnt|
  cnt.each do |table_name, row_count|
    total_counts[table_name] ||= 0
    total_counts[table_name] += row_count
    total_counts['rollup'] += row_count
  end
end

puts total_counts
