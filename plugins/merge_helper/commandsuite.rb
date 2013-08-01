# additional commands added by this plugin

require 'thor'

module Jetpants
  class CommandSuite < Thor

    desc 'merge_shards', 'Merge two or more shards using an aggregator instance'
    def merge_shards
      shards_to_merge = []
      aggregate_node
      spares_for_aggregate_shard
      shards.last.tables.each do |table| puts "172.16.122.228".to_db.mysql_root_cmd("use tumblr3;" + table.create_table_sql.gsub(/\t/,' ').gsub(/\n/,' ').gsub(/`/,'')); end
      aggregate_shard = new Shard(shards_to_merge.first.min_id, shards_to_merge.last.max_id, nil, :initializing)
      shards_to_merge.each do |shard|
        shard.children ||= [ aggregate_shard ]
      end

      sync_configuration
    end
  end
end

