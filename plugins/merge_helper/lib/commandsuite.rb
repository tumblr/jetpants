# additional commands added by this plugin

require 'thor'

module Jetpants
  class CommandSuite < Thor

    desc 'merge_shards', 'Merge two or more shards using an aggregator instance'
    def merge_shards
      min_id = ask("Please provide the min ID of the shard range to merge")
      max_id = ask("Please provide the max ID of the shard_range to merge")
      # for now we assume we'll never merge the shard at the head of the list
      shards_to_merge = shards.select{ |shard| (shard.min_id.to_i >= min_id.to_i && shard.max_id.to_i <= max_id.to_i && shard.max_id != 'INFINITY') }
      aggregate_node_ip = ask_node("Please supply the IP of an aggregator node")
      aggregate_node = Aggregator.new(aggregate_node_ip)

      # claim nodes for the new shard
      spares_for_aggregate_shard = Jetpants.topology.claim_spares(Jetpants.standby_slaves_per_pool + 1, role: :standby_slave, like: shards_to_merge.first.master)
      aggregate_shard_master = spares_for_aggregate_shard.pop

      # We need to make sure to sort shards in id range order
      aggregate_shard = Shard.new(shards_to_merge.first.min_id, shards_to_merge.last.max_id, aggregate_shard_master, :initializing)
      Jetpants.topology.pools << aggregate_shard 

      Shard.set_up_aggregate_node(shards_to_merge, aggregate_node, aggregate_shard_master)

      aggregate_node.start_all_slaves

      raise "There was an error initializing aggregate replication to some nodes, please verify all master" unless aggregate_node.all_replication_runing?

      # catch aggregate node up to data sources
      slaves_to_replicate.concurrent_each do |shard_slave|
        aggregate_node.aggregate_catch_up_to_master shard_slave
      end

      aggregate_shard_master.start_replication
      aggregate_shard_master.catch_up_to_master
      aggregate_shard_master.pause_replication

      # build up the rest of the new shard
      aggregate_shard_master.enslave! spares_for_aggregate_shard

      aggregate_shard.sync_configuration
    end

    # regenerate config and switch reads to new shard's master
    desc 'merge_shards_reads', 'Switch reads to the new merged master'
    def merge_shards_reads
      ask_merge_shards
      shards_to_merge.map(&:prepare_for_merged_reads)
      Jetpants.topology.write_config
    end

    # regenerate config and switch writes to new shard's master
    desc 'merge_shards_writes', 'Switch writes to the new merged master'
    def merge_shards_writes
      ask_merge_shards
      combined_shard = shards_to_merge.first.combined_shard
      shards_to_merge.map(&:prepare_for_merged_writes)
      combined_shard.state = :ready
      Jetpants.topology.write_config
    end

    # clean up aggregator node and old shards
    desc 'merge_shards_cleanup', 'Clean up the old shards and aggregator node'
    def merge_shards_cleanup
      ask_merge_shards
      shards_to_merge.map(&:decomission!)
    end

    no_tasks do
      def ask_merge_shards
        shards_to_merge = shards.select{ |shard| shard.combined_shard }
        shards_str = shards_to_merge.join(', ')
        answer = ask "Detected shards to merge as #{shard_str}, procede (enter YES in all caps if so)?"
        exit unless answer == "YES"
      end
    end
  end
end

