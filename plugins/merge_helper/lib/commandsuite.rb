# additional commands added by this plugin

require 'thor'

module Jetpants
  class CommandSuite < Thor

    desc 'merge_shards', 'Share merge step #1 of 4: Merge two or more shards using an aggregator instance'
    def merge_shards
      min_id = ask("Please provide the min ID of the shard range to merge:")
      max_id = ask("Please provide the max ID of the shard range to merge:")
      # for now we assume we'll never merge the shard at the head of the list
      shards_to_merge = Jetpants.shards.select{ |shard| (shard.min_id.to_i >= min_id.to_i && shard.max_id.to_i <= max_id.to_i && shard.max_id != 'INFINITY') }
      shard_str = shards_to_merge.join(', ')
      answer = ask "Detected shards to merge as #{shard_str}, proceed (enter YES in all caps if so)?:"
      raise "Aborting on user input" unless answer == "YES"

      aggregate_node_ip = ask "Please supply the IP of an aggregator node:"
      aggregate_node = Aggregator.new(aggregate_node_ip)
      raise "Invalide aggregate node!" unless aggregate_node.aggregator?

      # claim node for the new shard master
      spare_count = Jetpants.standby_slaves_per_pool + 1;
      raise "Not enough spares available!" unless Jetpants.count_spares(like: shards_to_merge.first.master) >= spare_count
      # claim the slaves further along in the process
      aggregate_shard_master = Jetpants.topology.claim_spare(role: :master, like: shards_to_merge.first.master)

      Shard.set_up_aggregate_node(shards_to_merge, aggregate_node, aggregate_shard_master)

      aggregate_node.resume_all_replication

      raise "There was an error initializing aggregate replication to some nodes, please verify all masters" unless aggregate_node.all_replication_running?
      raise "Count of aggregating nodes does not equal count of shards being merged" unless aggregate_node.aggregating_nodes.count == shards_to_merge.count

      aggregate_shard_master.start_replication

      # catch aggregate node up to data sources
      aggregate_node.aggregating_nodes.concurrent_each do |shard_slave|
        aggregate_node.aggregate_catch_up_to_master shard_slave
      end

      aggregate_shard_master.catch_up_to_master

      aggregate_shard = Shard.new(shards_to_merge.first.min_id, shards_to_merge.last.max_id, aggregate_shard_master, :initializing)
      Jetpants.topology.pools << aggregate_shard 
      # ensure a record is present in collins
      aggregate_shard.sync_configuration

      # build up the rest of the new shard
      spares_for_aggregate_shard = Jetpants.topology.claim_spares(Jetpants.standby_slaves_per_pool, role: :standby_slave, like: aggregate_node.aggregating_nodes.first)
      aggregate_shard_master.enslave! spares_for_aggregate_shard
      spares_for_aggregate_shard.concurrent_each(&:start_replication)
      spares_for_aggregate_shard.concurrent_each(&:catch_up_to_master)
      aggregate_shard_master.catch_up_to_master
      aggregate_shard_master.disable_read_only!

      # there is an implicit sync config here if you're using jetpants_collins
      aggregate_shard.state = :initialized
    end

    # regenerate config and switch reads to new shard's master
    desc 'merge_shards_reads', 'Share merge step #2 of 4: Switch reads to the new merged master'
    def merge_shards_reads
      shards_to_merge = ask_merge_shards
      validate_replication_stream shards_to_merge
      shards_to_merge.map(&:prepare_for_merged_reads)
      Jetpants.topology.write_config
    end

    # regenerate config and switch writes to new shard's master
    desc 'merge_shards_writes', 'Share merge step #3 of 4: Switch writes to the new merged master'
    def merge_shards_writes
      shards_to_merge = ask_merge_shards
      validate_replication_stream shards_to_merge
      combined_shard = shards_to_merge.first.combined_shard
      shards_to_merge.map(&:prepare_for_merged_writes)
      combined_shard.state = :ready
      combined_shard.sync_configuration
      Jetpants.topology.write_config
    end

    # clean up aggregator node and old shards
    desc 'merge_shards_cleanup', 'Share merge step #4 of 4: Clean up the old shards and aggregator node'
    def merge_shards_cleanup
      shards_to_merge = ask_merge_shards
      validate_replication_stream shards_to_merge
      combined_shard = shards_to_merge.first.combined_shard
      aggregator_host = combined_shard.master.master
      raise "Unexpected replication toplogy! Cannot find aggregator instance!" unless aggregator_host.aggregator?
      # currently there isn't a good way to automatically get aggregator objects back from normal topology traversal
      aggregator_instance = Aggregator.new(aggregator_host.ip)
      aggregator_instance.pause_all_replication
      aggregator_instance.remove_all_nodes!
      combined_shard.master.disable_replication!
      shards_to_merge.each do |shard|
        shard.master.enable_read_only!
      end
      shards_to_merge.map(&:decomission!)
    end

    desc 'validate_merge_replication', 'Validate replication streams for the nodes involved with a merge'
    def validate_merge_replication
      shards_to_merge = ask_merge_shards
      validate_replication_stream shards_to_merge
    end

    no_tasks do
      def ask_merge_shards
        shards_to_merge = Jetpants.shards.select{ |shard| !shard.combined_shard.nil? }
        shards_str = shards_to_merge.join(', ')
        answer = ask "Detected shards to merge as #{shards_str}, proceed (enter YES in all caps if so)?"
        raise "Aborting on user input" unless answer == "YES"

        shards_to_merge
      end

      def validate_replication_stream shards
        source_slaves = shards.map(&:slaves).flatten
        shards.each do |shard|
          shard.slaves.each do |slave|
            raise "Replication not running for #{slave} in #{slave.pool}!" unless slave.replicating?
            slave.output "Replication running for #{shard}"
          end
        end
        combined_shard = shards.last.combined_shard
        combined_shard.slaves.each do |slave|
          raise "Replication not running for #{slave} in #{slave.pool}!" unless slave.replicating?
          slave.output "Replication running for #{combined_shard}"
        end
        raise "Replication not running for #{combined_shard.master}!" unless combined_shard.master.replicating?
        combined_shard.master.output "Replication running for #{combined_shard} master"
        aggregator_host = combined_shard.master.master
        aggregator_instance = Aggregator.new(aggregator_host.ip)
        raise "Unexpected replication toplogy! Cannot find aggregator instance!" unless aggregator_host.aggregator?
        raise "Aggregator instance not replicating all data sources!" unless aggregator_instance.all_replication_running?
        aggregator_instance.aggregating_nodes.each do |shard_slave|
          raise "Aggregate data source #{shard_slave} not currently replicating!" unless shard_slave.replicating?
          raise "Aggregator replication source #{shard_slave} (#{shard_slave.pool}) not in list of shard slaves!" unless source_slaves.include? shard_slave
        end
        aggregator_instance.output "All replication streams running on merge node"
      end
    end
  end
end

