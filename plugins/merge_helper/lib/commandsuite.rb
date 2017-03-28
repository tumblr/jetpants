# additional commands added by this plugin

require 'thor'

module Jetpants
  class CommandSuite < Thor

    desc 'merge_shards_duplicate_check', 'Shard merge (step 0 of 5): perform the duplicate check on the shards being merged.'
    def merge_shards_duplicate_check
      # make sure we have a valid settings hash
      settings = Jetpants.plugins['merge_helper'] || {}

      raise "No table name specified to perform duplicate check" unless settings.has_key? 'table_dup_check'
      raise "No column name specified to perform duplicate check" unless settings.has_key? 'column_name_dup_check'

      # ask the user for the shards to merge
      shards_to_merge = ask_merge_shard_ranges

      min_key = settings['min_id_dup_check']
      max_key = settings['max_id_dup_check']

      table_name = settings['table_dup_check']
      column_name = settings['column_name_dup_check'].to_sym

      select_fields = [ settings['dup_check_fields'], settings['column_name_dup_check'] ].compact.flatten

      duplicates_found = false

      # Obtain all shard pairs for duplicate identification.
      shard_pairs = shards_to_merge.inject([[]]){|c,y|r=[];c.each{|i|r<<i;r<<i+[y]};r}.reject{|p| p.count != 2}

      shard_pairs.each { |shard_pair|
        source_shard = shard_pair[0]
        comparison_shard = shard_pair[1]
        source_db = source_shard.slaves.last
        table = source_shard.tables.select { |t| t.name == table_name }
        table = table.first
        key = column_name
        min_key_val = min_key
        max_key_val = max_key
        ids = Shard.check_duplicate_keys(shard_pair, table, key, min_key_val, max_key_val)

        if ids.length > 0
          duplicates_found = true
          pools = [source_shard, comparison_shard]
          source_db.output "Duplicate records and their data for pair: #{source_shard} and #{comparison_shard}"
          pools.concurrent_map { |pool|
            db = pool.standby_slaves.last || pool.backup_slaves.last

            # Query will output the results we need to fix.
            duplicates = db.query_return_array("SELECT #{select_fields.join(',')} FROM #{table_name} WHERE #{key} IN ( #{ids.join(',')} ) ORDER BY #{key}")
            output duplicates
          }
        end
      }
      output "Fix the duplicates manually before proceeding for the merge" if duplicates_found
    end
    def self.before_merge_shards_duplicate_check
      reminders(
        'This process may take several hours. You probably want to run this from a screen session.',
        'Be especially careful if you are relying on SSH Agent Forwarding for your root key, since this is not screen-friendly.'
      )
    end
    def self.after_merge_shards_duplicate_check
      reminders(
        'If the duplicates have been found, fix them first. Otherwise merge is going to fail.',
        'If no duplicates found, proceed to next step: jetpants merge_shards'
      )
    end

    desc 'merge_shards', 'Shard merge (step 1 of 5): merge two or more shards using an aggregator instance'
    method_option :old_way, :desc => 'Single threaded cloning of slaves of new master', :type => :boolean
    def merge_shards
      output "Make sure sender and receiver scripts are installed on source and targets. Refer README" unless options[:old_way]
      # ask the user for the shards to merge
      shards_to_merge = ask_merge_shard_ranges

      aggregate_node_ip = ask "Please supply the IP of an aggregator node:"
      aggregate_node = Aggregator.new(aggregate_node_ip)
      raise "Invalid aggregate node!" unless aggregate_node.aggregator?

      # claim the slaves further along in the process
      aggregate_shard_master_ip = ask("Enter the IP address of the new master or press enter to select a spare:")

      unless aggregate_shard_master_ip.empty?
         error "Node (#{aggregate_shard_master_ip.blue}) does not appear to be an IP address." unless is_ip? aggregate_shard_master_ip
         aggregate_shard_master = aggregate_shard_master_ip.to_db
         aggregate_shard_master.claim! if aggregate_shard_master.is_spare?
      else
         aggregate_shard_master = Jetpants.topology.claim_spare(role: :master, like: shards_to_merge.first.master)
      end

      # claim node for the new shard master
      spare_count = shards_to_merge.first.slaves_layout[:standby_slave];
      raise "Not enough spares available!" unless Jetpants.count_spares(like: aggregate_shard_master) >= spare_count
      raise "Not enough backup_slave role spare machines!" unless Jetpants.topology.count_spares(role: :backup_slave) >= shards_to_merge.first.slaves_layout[:backup_slave]

      # Perform cleanup on aggregator in case of any earlier unsuccessful merge
      if aggregate_node.needs_cleanup?
        answer = ask "Aggregator needs a cleanup.  Do you want to cleanup the aggregator? (enter YES in all caps if so)?:"
        if answer == "YES"
          aggregate_node.cleanup!
        else # Change state to Allocated:Spare again
          data_nodes.each(&:return_to_spare!)
          raise "Perform the aggregator cleanup manually and then restart the merge."
        end
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

      aggregate_shard = Shard.new(shards_to_merge.first.min_id, shards_to_merge.last.max_id, aggregate_shard_master, :initializing, shards_to_merge.first.shard_pool.name)
      # ensure a record is present in collins
      aggregate_shard.sync_configuration
      Jetpants.topology.add_pool aggregate_shard

      # build up the rest of the new shard
      spares_for_aggregate_shard = Jetpants.topology.claim_spares(aggregate_shard.slaves_layout[:standby_slave], role: :standby_slave, like: aggregate_shard_master, for_pool: aggregate_shard)
      backups_for_aggregate_shard = Jetpants.topology.claim_spares(aggregate_shard.slaves_layout[:backup_slave], role: :backup_slave, for_pool: aggregate_shard)

      spares_for_aggregate_shard = [spares_for_aggregate_shard, backups_for_aggregate_shard].flatten
      aggregate_shard_master.clone_multi_threaded = true unless options[:old_way]
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
    desc 'merge_shards_validate', 'Shard merge (step 2 of 5): validate aggregating server row counts'
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
    desc 'merge_shards_reads', 'Shard merge (step 3 of 5): switch reads to the new merged master'
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
        'Proceed to next step: jetpants merge_shards_writes'
      )
    end


    # regenerate config and switch writes to new shard's master
    desc 'merge_shards_writes', 'Shard merge (step 4 of 5): switch writes to the new merged master'
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
        'Proceed to next step: jetpants merge_shards_cleanup'
      )
    end

    # clean up aggregator node and old shards
    desc 'merge_shards_cleanup', 'Shard merge (step 5 of 5): clean up the old shards and aggregator node'
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
        'Review old nodes for hardware issues before re-using, or simply cancel them.'
      )
    end

    desc 'validate_merge_replication', 'Validate replication streams for the nodes involved with a merge'
    def validate_merge_replication
      shards_to_merge = ask_merge_shards
      validate_replication_stream shards_to_merge
    end

    no_tasks do
      def ask_merge_shards
        shard_pool_name = ask("Enter shard pool name performing a merge operation (enter for default #{Jetpants.topology.default_shard_pool}):")
        shard_pool_name = Jetpants.topology.default_shard_pool if shard_pool_name.empty?
        shards_to_merge = Jetpants.shards(shard_pool_name).select{ |shard| !shard.combined_shard.nil? }
        raise("No shards detected as merging!") if shards_to_merge.empty?
        shards_str = shards_to_merge.join(', ')
        answer = ask "Detected shards to merge as #{shards_str}, proceed (enter YES in all caps if so)?"
        raise "Aborting on user input" unless answer == "YES"

        shards_to_merge
      end

      def ask_merge_shard_ranges
        shard_pool = ask("Please enter the sharding pool which to perform the action on (enter for default pool #{Jetpants.topology.default_shard_pool}): ")
        shard_pool = Jetpants.topology.default_shard_pool if shard_pool.empty?

        min_id = ask("Please provide the min ID of the shard range to merge:")
        max_id = ask("Please provide the max ID of the shard range to merge:")

        # for now we assume we'll never merge the shard at the head of the list
        shards_to_merge = Jetpants.shards(shard_pool).select do |shard|
          shard.min_id.to_i >= min_id.to_i &&
          shard.max_id.to_i <= max_id.to_i &&
          shard.max_id != 'INFINITY'
        end

        shard_str = shards_to_merge.join(', ')
        answer = ask "Detected shards to merge as #{shard_str}, proceed (enter YES in all caps if so)?:"
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
