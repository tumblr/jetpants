require 'bloom-filter'

module Jetpants
  class Shard
    # Runs queries against a slave in the pool to verify sharding key values
    def validate_shard_data
      tables = Table.from_config('sharded_tables', shard_pool.name)
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

    # Uses a bloom filter to check for duplicate unique keys on two shards
    # @shards - an array of two shards
    # @table - the table object to examine
    # @key - the (symbol) of the key for which to verify uniqueness
    # @min_key_val - the minimum value of the key to consider
    # @max_key_val - the maximum value of the key to consider
    # @chunk_size - the number of values to retrieve in one query
    def self.check_duplicate_keys(shards, table, key, min_key_val = nil, max_key_val = nil, chunk_size = 5000)
      dbs = []
      shards.each do |shard|
        raise "Invalid shard #{shard}!" unless shard.is_a? Shard
        raise "Attempting to validate table not con" unless shard.has_table? table.name
      end
      raise "Currently only possible to compare 2 shards!" unless shards.count == 2
      raise "Invalid index '#{key}' for table '#{table}'!" if table.indexes[key].nil?
      raise "Only currently implemented for single-column indexes" unless table.indexes[key][:columns].count == 1

      source_shard = shards.first
      source_db = source_shard.standby_slaves.last
      comparison_shard = shards.last
      comparison_db = comparison_shard.standby_slaves.last
      column = table.indexes[key][:columns].first
      dbs = [ source_db, comparison_db ]

      dbs.concurrent_each do |db|
        db.pause_replication
        db.stop_query_killer
        db.disable_monitoring
      end

      min_val = min_key_val || source_db.query_return_first_value("SELECT min(#{column}) FROM #{table}")
      max_val = max_key_val || source_db.query_return_first_value("SELECT max(#{column}) FROM #{table}")

      # maximum possible entries and desired error rate
      max_size = (max_val.to_i - min_val.to_i) / 3
      filter = BloomFilter.new size: max_size
      curr_val = min_val

      source_db.output "Generating filter from #{source_shard} from values #{min_val}-#{max_val}"
      while curr_val < max_val do
        vals = source_db.query_return_array("SELECT #{column} FROM #{table} WHERE #{column} > #{curr_val} LIMIT #{chunk_size}").map{ |row| row.values.first }
        vals.each{ |val| filter.insert val }
        curr_val = vals.last
      end

      min_val = min_key_val || comparison_db.query_return_first_value("SELECT min(#{column}) FROM #{table}")
      max_val = max_key_val || comparison_db.query_return_first_value("SELECT max(#{column}) FROM #{table}")
      possible_dupes = []
      curr_val = min_val

      comparison_db.output "Searching for duplicates in #{comparison_shard} from values #{min_val}-#{max_val}"
      while curr_val < max_val do
        vals = comparison_db.query_return_array("SELECT #{column} FROM #{table} WHERE #{column} > #{curr_val} LIMIT #{chunk_size}").map{ |row| row.values.first }
        vals.each{ |val| possible_dupes << val if filter.include? val }
        curr_val = vals.last
      end

      if possible_dupes.empty?
        source_db.output "There were no duplicates"
      else
        source_db.output "There are #{possible_dupes.count} potential duplicates"
      end

      possible_dupes
    ensure
      unless dbs.empty?
        dbs.concurrent_each do |db|
          db.start_replication
          db.catch_up_to_master
          db.start_query_killer
          db.enable_monitoring
        end
      end
    end

    # Finds duplicate unique keys on two distinct shards
    #
    # @shards - an array of two shards
    # @table - the table object to examine
    # @key - the (symbol) of the key for which to verify uniqueness
    # @min_key_val - the minimum value of the key to consider
    # @max_key_val - the maximum value of the key to consider
    def self.find_duplicate_keys(shards, table, key, min_key_val = nil, max_key_val = nil)
      # check_duplicate_keys method will do all the validation of the parameters
      keys = Shard.check_duplicate_keys(shards, table, key, min_key_val, max_key_val)
      column = table.indexes[key][:columns].first

      keys.map do |k|
        count = shards.concurrent_map do |s|
          query = "select count(*) from #{table} where #{column} = #{k}"
          s.standby_slaves.last.query_return_first(query).values.first.to_i
        end.reduce(&:+)

        [k, count]
      end.select{ |f| f[1] > 1 }
    end

    # Generate a list of filenames for exported data
    def table_export_filenames(full_path = true, tables = false)
      export_filenames = []
      tables = Table.from_config('sharded_tables', shard_pool.name) unless tables
      export_filenames = tables.map { |table| table.export_filenames(@min_id, @max_id) }.flatten

      export_filenames.map!{ |filename| File.basename filename } unless full_path

      export_filenames
    end

    # Sets up an aggregate node and new shard master with data from two shards, returned with replication stopped
    # This will take two standby slaves, pause replication, export their data, ship it to the aggregate
    # node and new master, import the data, and set up multi-source replication to the shards being merged
    def self.set_up_aggregate_node(shards_to_merge, aggregate_node, new_shard_master)
      # validation
      shards_to_merge.each do |shard|
        raise "Attempting to create an aggregate node with a non-shard!" unless shard.is_a? Shard
      end
      raise "Attempting to set up aggregation on a non-aggregate node!" unless aggregate_node.aggregator?
      raise "Attempting to set up aggregation on a node that is already aggregating!" unless aggregate_node.aggregating_nodes.empty?
      raise "Invalid new master node!" unless new_shard_master.is_a? DB
      raise "New shard master already has a pool!" unless new_shard_master.pool.nil?

      data_nodes = [ new_shard_master, aggregate_node ]

      # create and ship schema.  Mysql is stopped so that we can use buffer pool memory during network copy on destinations
      slave = shards_to_merge.last.standby_slaves.last
      data_nodes.each do |db|
        db.stop_mysql
        slave.ship_schema_to db
      end

      # grab slave list to export data
      slaves_to_replicate = shards_to_merge.map { |shard| shard.standby_slaves.last }

      # sharded table list to ship
      tables = Plugin::MergeHelper.tables_to_merge(shards_to_merge.first.shard_pool.name)

      # data export counts for validation later
      export_counts = {}
      slave_coords = {}

      # concurrency controls for export/transfer
      transfer_lock = Mutex.new

      # asynchronously export data on all slaves
      slaves_to_replicate.concurrent_map { |slave|
        # these get cleaned up further down after replication is set up
        slave.disable_monitoring
        slave.set_downtime 12
        slave.stop_query_killer
        slave.pause_replication

        slave.export_data tables, slave.pool.min_id, slave.pool.max_id
        # record export counts for validation
        export_counts[slave] = slave.import_export_counts
        # retain coords to set up replication hierarchy
        file, pos = slave.binlog_coordinates
        slave_coords[slave] = { :log_file => file, :log_pos => pos }

        transfer_lock.synchronize do
          slave.fast_copy_chain(
            Jetpants.export_location,
            data_nodes,
            port: 3307,
            files: slave.pool.table_export_filenames(full_path = false, tables),
            overwrite: true
          )
        end
        # clean up files on origin slave
        slave.output "Cleaning up export files..."
        slave.pool.table_export_filenames(full_path = true, tables).map { |file|
          slave.ssh_cmd("rm -f #{file}")
        }

        # restart origin slave replication
        slave.resume_replication
        slave.catch_up_to_master
        slave.enable_monitoring
        slave.start_query_killer
        slave.cancel_downtime rescue nil
      }

      # settings to improve import speed
      data_nodes.each do |db|
        db.start_mysql '--skip-log-bin', '--skip-log-slave-updates', '--innodb-autoinc-lock-mode=2', '--skip-slave-start', '--innodb_flush_log_at_trx_commit=2', '--innodb-doublewrite=0'
        db.import_schemata!
      end

      # import data in a separate loop, as we want to leave the origin slaves
      # in a non-replicating state for as little time as possible
      data_nodes.concurrent_map { |db|
        # load data and inject export counts from earlier for validation
        slaves_to_replicate.map { |slave| 
          db.inject_counts export_counts[slave]
          db.import_data tables, slave.pool.min_id, slave.pool.max_id
        }
      }

      # clear out earlier import options
      data_nodes.concurrent_each do |db|
        db.restart_mysql "--skip-slave-start"
      end

      # set up replication hierarchy
      slaves_to_replicate.each do |slave|
        aggregate_node.add_node_to_aggregate slave, slave_coords[slave]
      end
      new_shard_master.change_master_to aggregate_node
    end

    def combined_shard
      Jetpants.shards(shard_pool.name).select { |shard| ( 
        shard.min_id.to_i <= @min_id.to_i \
        && shard.max_id.to_i >= @max_id.to_i \
        && shard.max_id != 'INFINITY' \
        && @max_id != 'INFINITY' \
        && (shard.state == :initialized || shard.state == :ready) \
        && shard != self
      )}.first
    end

    def prepare_for_merged_reads
      @state = :merging
      sync_configuration
    end

    def prepare_for_merged_writes
      @state = :deprecated
      sync_configuration
    end

    def decomission!
      # trigger the logic in the jetpants collins helper to eject the boxes in the cluster
      @state = :recycle
      sync_configuration
    end

    def in_config?
      [:merging, :ready, :child, :needs_cleanup, :read_only, :offline].include? @state
    end
  end
end
