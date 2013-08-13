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

      total_export_counts = {}
      total_import_counts = {}
      slaves_to_replicate = []

      # settings to improve import speed
      aggregate_node.restart_mysql '--skip-log-bin', '--skip-log-slave-updates', '--innodb-autoinc-lock-mode=2', '--skip-slave-start'
      tables = Table.from_config 'sharded_tables'

      # create and ship schema
      slave = shards_to_merge.last.standby_slaves.last
      slave.ship_schema_to aggregate_node
      aggregate_node.import_schemata!

      # grab slave list to export data
      slaves_to_replicate = shards_to_merge.map { |shard| shard.standby_slaves.last }

      # asynchronously export data on all slaves
      slaves_to_replicate.concurrent_each do |slave|
        # these get cleaned up further down after replication is set up
        slave.disable_monitoring
        slave.stop_query_killer
        slave.pause_replication

        export_counts = slave.export_data tables, slave.pool.min_id, slave.pool.max_id

        if total_export_counts.empty?
          total_export_counts = export_counts
        else
          total_export_counts.keys.each do |key|
            total_export_counts[key] = total_export_counts[key] + export_counts[key]
          end
        end
      end

      # iterate through and load data from each slave
      slaves_to_repliate.each do |slave|
        
        slave.fast_copy_chain(
          Jetpants.export_location,
          aggregate_node,
          port: 3307,
          files: slave.pool.table_export_filenames(full_path = false),
          overwrite: true
        )

        import_counts = aggregate_node.import_data tables, slave.pool.min_id, slave.pool.max_id
        if total_import_counts.empty?
          total_import_counts = import_counts
        else
          total_import_counts.keys.each do |key|
            total_import_counts[key] = total_import_counts[key] + import_counts[key]
          end
        end
      end

      # clear out earlier import options
      aggregate_node.restart_mysql

      # validate import counts
      raise "Imported and exported table count doesn't match!" unless total_import_counts.keys.count == total_export_counts.keys.count
      valid = true;
      total_import_count.each do |key, val|
        if val != total_export_count[key]
          output "Count for export/import of #{key} is wrong! (#{val} imported #{total_export_count[key]} exported)"
          valid = false
        end
      end

      raise "Import/export counts do not match, aborting" unless valid

      # resume replication on source nodes and catch up to master
      aggregate_node.add_nodes_to_aggregate slaves_to_replicate
      slaves_to_replicate.concurrent_each do |slave|
        slave.resume_replication
        slave.catch_up_to_master
        slave.enable_monitoring
        slave.start_query_killer
      end

      aggregate_node.start_all_slaves

      raise "There was an error initializing aggregate replication to some nodes, please verify all master" unless aggregate_node.all_replication_runing?

      # catch aggregate node up to data sources
      slaves_to_replicate.each do |shard_slave|
        aggregate_node.aggregate_catch_up_to_master shard_slave
      end

      # claim nodes for the new shard
      spares_for_aggregate_shard = Jetpants.topology.claim_spares(Jetpants.standby_slaves_per_pool + 1, role: :standby_slave, like: shards_to_merge.first.master)
      aggregate_shard_master = spares_for_aggregate_shard.pop

      # export and ship schema to new aggregated shard master
      aggregate_node.export_schemata tables
      aggregate_node.ship_schema_to aggregate_shard_master
      aggregate_shard_master.import_schemata!

      # export and ship data to new shard master
      aggregate_export_counts = aggregate_node.export_data tables, shards_to_merge.first.min_id, shards_to_merge.last.max_id
      aggregate_node.fast_copy_chain(
        Jetpants.export_location,
        aggregate_shard_master,
        port: 3307,
        files: aggregate_shard.table_export_filenames(full_path = false),
        overwrite: true
      )
      aggregate_import_counts = aggregate_shard_master.import_data tables, shards_to_merge.first.min_id, shards_to_merge.last.max_id

      # validate counts from load_data_infile / select_into_outfile
      valid_import = true
      aggregate_export_counts.each do |key, count|
        if count != aggregate_import_counts[key]
          output "Count for aggregate export/import of #{key} is wrong! (#{aggregate_import_counts[key]} imported #{count} exported)"
          valid_import = false
        end
      end
      raise "Import/export counts do not match, aborting" unless valid_import

      # build up the rest of the new shard
      aggregate_shard_master.enslave! spares_for_aggregate_shard

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

