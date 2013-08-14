# additional commands added by this plugin

require 'thor'

module Jetpants
  class CommandSuite < Thor

    desc 'merge_shards', 'Merge two or more shards using an aggregator instance'
    def merge_shards
      shards_to_merge = []
      aggregate_node
      # We need to make sure to sort shards in id range order
      aggregate_shard = Shard.new(shards_to_merge.first.min_id, shards_to_merge.last.max_id, aggregate_node, :initializing)
      Jetpants.topology.pools << aggregate_shard 

      aggregate_node = Shard.set_up_aggregate_node(shards_to_merge, aggregate_node_id)

      aggregate_node.start_all_slaves

      raise "There was an error initializing aggregate replication to some nodes, please verify all master" unless aggregate_node.all_replication_runing?

      # catch aggregate node up to data sources
      slaves_to_replicate.each do |shard_slave|
        aggregate_node.aggregate_catch_up_to_master shard_slave
      end

      # claim nodes for the new shard
      spares_for_aggregate_shard = Jetpants.topology.claim_spares(Jetpants.standby_slaves_per_pool + 1, role: :standby_slave, like: shards_to_merge.first.master)
      aggregate_shard_master = spares_for_aggregate_shard.pop

      Shard.ship_aggregate_data_to_new_master(aggregate_node, new_shard_master)

      # build up the rest of the new shard
      aggregate_shard_master.enslave! spares_for_aggregate_shard
      aggregate_shard.master = aggregate_shard_master

      sync_configuration
    end

    # regenerate config and switch reads to new shard's master
    desc 'merge_shards_reads', 'Switch reads to the new parent master'
    def merge_shards_reads
    end

    # regenerate config and switch writes to new shard's master
    desc 'merge_shards_writes', 'Switch writes to the new parent master'
    def merge_shards_writes
    end

    # clean up aggregator node and old shards
    desc 'merge_shards_cleanup', 'Clean up the old shards and aggregator node'
    def merge_shards_cleanup
    end
  end
end

