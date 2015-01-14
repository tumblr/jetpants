# additional commands added by this plugin

require 'thor'

module Jetpants
  class CommandSuite < Thor

    desc 'merge_shards', 'Share merge step #1 of 5: Merge two or more shards using an aggregator instance'
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
      raise "Invalid aggregate node!" unless aggregate_node.aggregator?

      # claim node for the new shard master
      spare_count = shards_to_merge.first.slaves_layout[:standby_slave] + 1;
      raise "Not enough spares available!" unless Jetpants.count_spares(like: shards_to_merge.first.master) >= spare_count
      raise "Not enough backup_slave role spare machines!" unless Jetpants.topology.count_spares(role: :backup_slave) >= shards_to_merge.first.slaves_layout[:backup_slave]

      # claim the slaves further along in the process
      aggregate_shard_master = ask_node("Enter the IP address of the new master or press enter to select a spare:")

      if aggregate_shard_master
         aggregate_shard_master.claim! if aggregate_shard_master.is_spare?
      else
         aggregate_shard_master = Jetpants.topology.claim_spare(role: :master, like: shards_to_merge.first.master)
      end

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
      # ensure a record is present in collins
      aggregate_shard.sync_configuration
      Jetpants.topology.add_pool aggregate_shard

      # build up the rest of the new shard
      spares_for_aggregate_shard = Jetpants.topology.claim_spares(aggregate_shard.slaves_layout[:standby_slave], role: :standby_slave, like: aggregate_shard_master)
      backups_for_aggregate_shard = Jetpants.topology.claim_spares(aggregate_shard.slaves_layout[:backup_slave], role: :backup_slave)

      spares_for_aggregate_shard = [spares_for_aggregate_shard, backups_for_aggregate_shard].flatten
      aggregate_shard_master.enslave! spares_for_aggregate_shard
      spares_for_aggregate_shard.concurrent_each(&:start_replication)
      spares_for_aggregate_shard.concurrent_each(&:catch_up_to_master)
      aggregate_shard_master.catch_up_to_master
      aggregate_shard_master.disable_read_only!

      # there is an implicit sync config here if you're using jetpants_collins
      aggregate_shard.state = :initialized
    end
    def self.before_merge_shards
      reminders(
        'This process may take several hours. You probably want to run this from a screen session.',
        'Be especially careful if you are relying on SSH Agent Forwarding for your root key, since this is not screen-friendly.'
      )
    end
    def self.after_merge_shards
      reminders(
        'Proceed to next step: jetpants merge_shards_validate'
      )
    end

    # Performs a validation step of pausing replication and determining row counts
    # on the aggregating server and its data sources
    desc 'merge_shards_validate', 'Share merge step #2 of 5: Validate aggregating server row counts'
    def merge_shards_validate
      # obtain relevant shards
      shards_to_merge = ask_merge_shards
      combined_shard = shards_to_merge.first.combined_shard

      # validate topology and state
      raise "Combined shard #{combined_shard} in unexpected state #{combined_shard.state}, should be 'initialized'" unless combined_shard.state == :initialized
      shards_to_merge.each do |shard|
        raise "Shard to merge #{shard} in unexpected state #{shard.state}, should be 'ready'" unless shard.state == :ready
      end
      validate_replication_stream shards_to_merge

      # we need to refresh Jetpants because of side effects with Aggregator
      # https://jira.ewr01.tumblr.net/browse/DATASRE-714
      Jetpants.refresh

      aggregator_host = combined_shard.master.master
      aggregator_instance = Aggregator.new(aggregator_host.ip)

      unless aggregator_instance.validate_aggregate_row_counts
        raise "Aggregating node's row count does not add up to the sum from the source shards"
      end
    end
    def self.before_merge_shards_validate
      reminders(
          'This process may take some time. You probably want to run this from a screen session.',
          'Be especially careful if you are relying on SSH Agent Forwarding for your root key, since this is not screen-friendly.'
      )
    end
    def self.after_merge_shards_validate
      reminders(
          'Proceed to next step: jetpants merge_shards_reads'
      )
    end

    # regenerate config and switch reads to new shard's master
    desc 'merge_shards_reads', 'Share merge step #3 of 5: Switch reads to the new merged master'
    def merge_shards_reads
      # obtain relevant shards
      shards_to_merge = ask_merge_shards
      combined_shard = shards_to_merge.first.combined_shard

      # validate topology and state
      raise "Combined shard #{combined_shard} in unexpected state #{combined_shard.state}, should be 'initialized'" unless combined_shard.state == :initialized
      shards_to_merge.each do |shard|
        raise "Shard to merge #{shard} in unexpected state #{shard.state}, should be 'ready'" unless shard.state == :ready
      end
      validate_replication_stream shards_to_merge

      # manipulate state for reads and write config
      shards_to_merge.map(&:prepare_for_merged_reads)
      Jetpants.topology.write_config
    end
    def self.after_merge_shards_reads
      reminders(
        'Commit/push the configuration in version control.',
        'Deploy the configuration to all machines.',
        'Wait for reads to stop on the old parent masters.',
        'Proceed to next step: jetpants merge_shards_writes',
      )
    end


    # regenerate config and switch writes to new shard's master
    desc 'merge_shards_writes', 'Share merge step #4 of 5: Switch writes to the new merged master'
    def merge_shards_writes
      # obtain relevant shards
      shards_to_merge = ask_merge_shards
      combined_shard = shards_to_merge.first.combined_shard

      # perform topology/replication and state validation
      validate_replication_stream shards_to_merge
      raise "Combined shard #{combined_shard} in unexpected state #{combined_shard.state}, should be 'initialized'" unless combined_shard.state == :initialized
      shards_to_merge.each do |shard|
        raise "Shard to merge #{shard} in unexpected state #{shard.state}, should be 'merging'" unless shard.state == :merging
      end

      # perform state manipulation for writes and write config
      shards_to_merge.map(&:prepare_for_merged_writes)
      combined_shard.state = :ready
      combined_shard.sync_configuration
      Jetpants.topology.write_config
    end
    def self.after_merge_shards_writes
      reminders(
        'Commit/push the configuration in version control.',
        'Deploy the configuration to all machines.',
        'Wait for writes to stop on the old parent masters.',
        'Proceed to next step: jetpants merge_shards_cleanup',
      )
    end

    # clean up aggregator node and old shards
    desc 'merge_shards_cleanup', 'Share merge step #5 of 5: Clean up the old shards and aggregator node'
    def merge_shards_cleanup
      # obtain relevant shards
      shards_to_merge = ask_merge_shards
      combined_shard = shards_to_merge.first.combined_shard

      # perform topology and replication validation
      validate_replication_stream shards_to_merge
      raise "Combined shard #{combined_shard} in unexpected state #{combined_shard.state}, should be 'ready'" unless combined_shard.state == :ready
      shards_to_merge.each do |shard|
        raise "Shard to merge #{shard} in unexpected state #{shard.state}, should be 'deprecated'" unless shard.state == :deprecated
      end
      aggregator_host = combined_shard.master.master
      raise "Unexpected replication toplogy! Cannot find aggregator instance!" unless aggregator_host.aggregator?

      # tear down aggregate replication and clean up state
      aggregator_instance = Aggregator.new(aggregator_host.ip)
      aggregator_instance.pause_all_replication
      aggregator_instance.remove_all_nodes!
      combined_shard.master.disable_replication!
      shards_to_merge.each do |shard|
        shard.master.enable_read_only!
      end
      shards_to_merge.map(&:decomission!)
    end
    def self.after_merge_shards_cleanup
      reminders(
        'Review old nodes for hardware issues before re-using, or simply cancel them.',
      )
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
        raise "Unexpected replication topology! Cannot find aggregator instance!" unless aggregator_host.aggregator?
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
