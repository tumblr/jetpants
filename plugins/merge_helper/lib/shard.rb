module Jetpants
  class Shard
    # Runs queries against a slave in the pool to verify sharding key values
    def validate_shard_data
      tables = Table.from_config 'sharded_tables'
      table_statuses = {}
      tables.limited_concurrent_map(8) { |table|
        table.sharding_keys.each do |col|
          range_sql = table.sql_range_check col, @min_id, @max_id

          # use a standby slave, since this query will be very heavy and these shards are live
          db = standby_slaves.last
          result = db.query_return_array range_sql

          if result.first.values.first > 0
            table_statuses[table] = :invalid
          else
            table_statuses[table] = :valid
          end
        end
      }

      table_statuses
    end

    # Generate a list of filenames for exported data
    def table_export_filenames(full_path = true)
      export_filenames = []
      tables = Table.from_config 'sharded_tables'
      export_filenames = tables.map { |table| table.export_filenames(@min_id, @max_id) }.flatten

      export_filenames.map!{ |filename| File.basename filename } unless full_path

      export_filenames
    end

    # Sets up an aggregate node and new shard master with data from two shards, returned with replication stopped
    # This will take two standby slaves, pause replication, export their data, ship it to the aggregate
    # node and new master, import the data, and set up multi-source replication to the shards being merged
    def self.set_up_aggregate_node(shards_to_merge, aggregate_node, new_shard_master)
      shards_to_merge.each do |shard|
        raise "Attempting to create an aggregate node with a non-shard!" unless shard.is_a? Shard
      end
      raise "Attempting to set up aggregation on a non-aggregate node!" unless aggregate_node.aggregator?
      raise "Attempting to set up aggregation on a node that is already aggregating!" unless aggregate_node.aggregating_nodes.empty?

      data_nodes = [ new_shard_master, aggregate_node ]

      # settings to improve import speed
      data_nodes.concurrent_each do |db|
        db.restart_mysql '--skip-log-bin', '--skip-log-slave-updates', '--innodb-autoinc-lock-mode=2', '--skip-slave-start', '--innodb_flush_log_at_trx_commit=2'
      end

      # create and ship schema
      slave = shards_to_merge.last.standby_slaves.last
      data_nodes.each do |db|
        slave.ship_schema_to db
        db.import_schemata!
      end

      # grab slave list to export data
      slaves_to_replicate = shards_to_merge.map { |shard| shard.standby_slaves.last }

      # sharded table list to ship
      tables = Table.from_config 'sharded_tables'

      total_export_counts = {}

      # asynchronously export data on all slaves
      slaves_to_replicate.concurrent_map { |slave|
        # these get cleaned up further down after replication is set up
        slave.disable_monitoring
        slave.stop_query_killer
        slave.pause_replication

        export_counts = slave.export_data tables, slave.pool.min_id, slave.pool.max_id
      }.each do |export_counts|
        export_counts.keys.each do |key|
          total_export_counts[key] ||= 0
          total_export_counts[key] = total_export_counts[key] + export_counts[key]
        end
      end

      total_import_counts = {}

      # ship and load data from each slave
      slaves_to_repliate.map { |slave|
        slave.fast_copy_chain(
          Jetpants.export_location,
          data_nodes,
          port: 3307,
          files: slave.pool.table_export_filenames(full_path = false),
          overwrite: true
        )

        datanode_counts = Hash [
          data_nodes.concurrent_map { |db|
            import_counts = db.import_data tables, slave.pool.min_id, slave.pool.max_id
            [ db, import_counts ]
          }
        ]
      }.each do |node, import_counts|
        total_export_counts.keys.each do |key|
          total_import_counts[key] ||= 0
          total_import_counts[key] = total_import_counts[key] + import_counts[key]
        end
      end

      # clear out earlier import options
      data_nodes.concurrent_each do |db|
        aggregate_node.restart_mysql "--skip-start-slave"
      end

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
      new_shard_master.change_master_to aggregate_node
      slaves_to_replicate.concurrent_each do |slave|
        slave.resume_replication
        slave.catch_up_to_master
        slave.enable_monitoring
        slave.start_query_killer
      end

      aggregate_node
    end

    # Exports data from an aggregate node via SELECT INTO OUTFILE, ships the data to a node
    # which is to be the merged shard master, and sets up replication on the new shard master to
    # the aggregate node
    def self.ship_aggregate_data_to_new_master(aggregate_node, new_shard_master, aggregate_shard)
      # binlog coords to resume replication
      aggregate_node.stop_all_replication
      coords = aggregate_node.binlog_coords

      # export and ship schema to new aggregated shard master
      aggregate_node.export_schemata tables
      aggregate_node.ship_schema_to aggregate_shard_master
      aggregate_shard_master.import_schemata!

      # export and ship data to new shard master
      aggregate_node.stop_all_replication
      aggregate_export_counts = aggregate_node.export_data tables, shards_to_merge.first.min_id, shards_to_merge.last.max_id
      aggregate_node.fast_copy_chain(
        Jetpants.export_location,
        aggregate_shard_master,
        port: 3307,
        files: aggregate_shard.table_export_filenames(full_path = false),
        overwrite: true
      )
      aggregate_shard_master.restart_mysql '--skip-log-bin', '--skip-log-slave-updates', '--innodb-autoinc-lock-mode=2', '--skip-slave-start'
      aggregate_shard_master.stop_query_killer
      aggregate_shard_master.disable_monitoring
      aggregate_import_counts = aggregate_shard_master.import_data tables, shards_to_merge.first.min_id, shards_to_merge.last.max_id
      aggregate_shard_master.restart_mysql
      aggregate_shard_master.start_query_killer
      aggregate_shard_master.enable_monitoring

      # validate counts from load_data_infile / select_into_outfile
      valid_import = true
      aggregate_export_counts.each do |key, count|
        if count != aggregate_import_counts[key]
          output "Count for aggregate export/import of #{key} is wrong! (#{aggregate_import_counts[key]} imported #{count} exported)"
          valid_import = false
        end
      end
      raise "Import/export counts do not match, aborting" unless valid_import

      aggregate_shard_master.change_master_to aggregate_node, coords
      aggregate_node.start_all_slaves
      # catch aggregate node up to data sources
      slaves_to_replicate.each do |shard_slave|
        aggregate_node.aggregate_catch_up_to_master shard_slave
      end
    end
  end
end
