module Jetpants

  #--
  # Import, export, and data set methods #######################################
  #++

  class DB
    # Exports the DROP TABLE + CREATE TABLE statements for the given tables via mysqldump
    def export_schemata(tables)
      output 'Exporting table definitions'
      supply_root_pw = (Jetpants.mysql_root_password ? "-p#{Jetpants.mysql_root_password}" : '')
      supply_port = (@port == 3306 ? '' : "-h 127.0.0.1 -P #{@port}")
      cmd = "mysqldump #{supply_root_pw} #{supply_port} -d #{app_schema} " + tables.join(' ') + " >#{Jetpants.export_location}/create_tables_#{@port}.sql"
      cmd.untaint
      result = ssh_cmd(cmd)
      output result
    end

    # Executes a .sql file previously created via export_schemata.
    # Warning: this will DESTROY AND RECREATE any tables contained in the file.
    # DO NOT USE ON A DATABASE THAT CONTAINS REAL DATA!!! This method doesn't
    # check first! The statements will replicate to any slaves! PROCEED WITH
    # CAUTION IF RUNNING THIS MANUALLY!
    def import_schemata!
      output 'Dropping and re-creating table definitions'
      result = mysql_root_cmd "source #{Jetpants.export_location}/create_tables_#{@port}.sql", terminator: '', schema: true
      output result
    end

    # Has no built-in effect. Plugins can override this and/or use before_alter_schemata
    # and after_alter_schemata callbacks to provide an implementation.
    # Also sometimes useful to override this as a singleton method on specific DB objects
    # in a migration script.
    def alter_schemata
    end

    # Exports data for the supplied tables. If min/max ID supplied, only exports
    # data where at least one of the table's sharding keys falls within this range.
    # Creates a 'jetpants' db user with FILE permissions for the duration of the
    # export.
    def export_data(tables, min_id=false, max_id=false, infinity=false)
      pause_replication if @master && ! @repl_paused
      import_export_user = 'jetpants'
      create_user(import_export_user)
      grant_privileges(import_export_user)               # standard privs
      grant_privileges(import_export_user, '*', 'FILE')  # FILE global privs
      reconnect(user: import_export_user)
      @counts ||= {}
      tables.each {|t| @counts[t.name] = export_table_data t, min_id, max_id, infinity}
    ensure
      reconnect(user: app_credentials[:user])
      drop_user import_export_user
    end

    # Exports data for a table. Only includes the data subset that falls
    # within min_id and max_id. The export files will be located according
    # to the export_location configuration setting.
    # Returns the number of rows exported.
    def export_table_data(table, min_id=false, max_id=false, infinity=false)
      unless min_id && max_id && table.chunks > 0
        output "Exporting all data", table
        rows_exported = query(table.sql_export_all)
        output "#{rows_exported} rows exported", table
        return rows_exported
      end

      output "Exporting data for ID range #{min_id}..#{max_id}", table
      lock = Mutex.new
      rows_exported = 0
      chunks_completed = 0

      (min_id..max_id).in_chunks(table.chunks) do |min, max|
        attempts = 0
        begin
          sql = table.sql_export_range(min, max)
          result = query sql
          lock.synchronize do
            rows_exported += result
            chunks_completed += 1
            percent_finished = 100 * chunks_completed / table.chunks
            output("Export #{percent_finished}% complete.", table) if table.chunks >= 40 && chunks_completed % 20 == 0
          end
        rescue => ex
          if attempts >= 10
            output "EXPORT ERROR: #{ex.message}, chunk #{min}-#{max}, giving up", table
            raise
          end
          attempts += 1
          output "EXPORT ERROR: #{ex.message}, chunk #{min}-#{max}, attempt #{attempts}, re-trying after delay", table
          ssh_cmd("rm -f " + table.export_file_path(min, max))
          sleep(1.0 * attempts)
          retry
        end
      end

      if infinity
        attempts = 0
        begin
          output("Exporting infinity range.", table)
          infinity_rows_exported = query(table.sql_export_range(max_id+1, false))
          rows_exported += infinity_rows_exported
          output("Export of infinity range complete.", table)
        rescue => ex
          if attempts >= 10
            output "EXPORT ERROR: #{ex.message}, chunk #{max_id+1}-INFINITY, giving up", table
            raise
          end
          attempts += 1
          output "EXPORT ERROR: #{ex.message}, chunk #{max_id+1}-INFINITY, attempt #{attempts}, re-trying after delay", table
          ssh_cmd("rm -f " + table.export_file_path(max_id+1, false))
          sleep(1.0 * attempts)
          retry
        end
      end

      output "#{rows_exported} rows exported", table
      rows_exported
    end

    def highest_table_key_value(table, key=nil)
      key = table.first_pk_col unless key
      return query_return_first_value("SELECT max(#{key}) from #{table.name};")
    end

    # Imports data for a table that was previously exported using export_data.
    # Only includes the data subset that falls within min_id and max_id.  If
    # run after export_data (in the same process), import_data will
    # automatically confirm that the import counts match the previous export
    # counts.
    #
    # Creates a 'jetpants' db user with FILE permissions for the duration of the
    # import.
    #
    # Note: the caller must disable binary logging (for speed reasons and to
    # avoid potential GTID problems with complex operations) and set InnoDB
    # autoinc lock mode to 2 (to support chunking of auto-inc tables) prior to
    # calling DB#import_data. This is the caller's responsibility, and can be
    # achieved by calling DB#restart_mysql with appropriate option overrides
    # prior to importing data. After done importing, the caller can clear those
    # settings by calling DB#restart_mysql again with no params.
    def import_data(tables, min_id=false, max_id=false, infinity=false, extra_opts=nil)
      raise "Binary logging must be disabled prior to calling DB#import_data" if binary_log_enabled?
      disable_read_only!
      import_export_user = 'jetpants'
      create_user(import_export_user)
      grant_privileges(import_export_user)               # standard privs
      grant_privileges(import_export_user, '*', 'FILE')  # FILE global privs

      # Disable unique checks upon connecting. This has to be done at the :after_connect level in Sequel
      # to guarantee it's being run on every connection in the conn pool. This is mysql2-specific.
      disable_unique_checks_proc = Proc.new {|mysql2_client| mysql2_client.query 'SET unique_checks = 0'}

      reconnect(user: import_export_user, after_connect: disable_unique_checks_proc)

      import_counts = {}

      tables.each {|t| import_counts[t.name] = import_table_data t, min_id, max_id, infinity, extra_opts}

      # Verify counts
      @counts ||= {}
      @counts.each do |name, exported|
        if exported == import_counts[name]
          output "Verified import count matches export count for table #{name}"
        else
          raise "Import count (#{import_counts[name]}) does not match export count (#{exported}) for table #{name}"
        end
      end

    ensure
      reconnect(user: app_credentials[:user])
      drop_user(import_export_user)
    end

    # Imports the data subset previously dumped through export_data.
    # Returns number of rows imported.
    def import_table_data(table, min_id=false, max_id=false, infinity=false, extra_opts=nil)
      unless min_id && max_id && table.chunks > 0
        output "Importing all data", table
        rows_imported = query(table.sql_import_all)
        output "#{rows_imported} rows imported", table
        return rows_imported
      end

      output "Importing data for ID range #{min_id}..#{max_id}", table
      lock = Mutex.new
      rows_imported = 0
      chunks_completed = 0

      (min_id..max_id).in_chunks(table.chunks) do |min, max|
        attempts = 0
        begin
          sql = table.sql_import_range(min, max, extra_opts)
          result = query sql
          lock.synchronize do
            rows_imported += result
            chunks_completed += 1
            percent_finished = 100 * chunks_completed / table.chunks
            output("Import #{percent_finished}% complete.", table) if table.chunks >= 40 && chunks_completed % 20 == 0
            chunk_file_name = table.export_file_path(min, max)
            ssh_cmd "rm -f #{chunk_file_name}"
          end
        rescue => ex
          if attempts >= 10
            output "IMPORT ERROR: #{ex.message}, chunk #{min}-#{max}, giving up", table
            raise
          end
          attempts += 1
          output "IMPORT ERROR: #{ex.message}, chunk #{min}-#{max}, attempt #{attempts}, re-trying after delay", table
          sleep(3.0 * attempts)
          retry
        end
      end

      if infinity
        attempts = 0
        begin
          infinity_rows_imported = query(table.sql_import_range(max_id+1, false))
          output("Importing infinity range", table)
          chunk_file_name = table.export_file_path(max_id+1, false)
          ssh_cmd "rm -f #{chunk_file_name}"
          rows_imported += infinity_rows_imported
          output("Import of infinity range complete", table)
        rescue => ex
          if attempts >= 10
            output "IMPORT ERROR: #{ex.message}, chunk #{max_id+1}-INFINITY, giving up", table
            raise
          end
          attempts += 1
          output "IMPORT ERROR: #{ex.message}, chunk #{max_id+1}-INFINITY, attempt #{attempts}, re-trying after delay", table
          sleep(3.0 * attempts)
          retry
        end
      end

      output "#{rows_imported} rows imported", table
      rows_imported
    end

    # Counts rows falling between min_id and max_id for the supplied tables.
    # Returns a hash mapping table names to counts.
    # Note: runs 10 concurrent queries to perform the count quickly. This is
    # MUCH faster than doing a single count, but far more I/O intensive, so
    # don't use this on a master or active slave.
    def row_counts(tables, min_id, max_id)
      tables = [tables] unless tables.is_a? Array
      lock = Mutex.new
      row_count = {}
      tables.each do |t|
        row_count[t.name] = 0
        if min_id && max_id && t.chunks > 1
          (min_id..max_id).in_chunks(t.chunks, Jetpants.max_concurrency) do |min, max|
            result = query_return_first_value(t.sql_count_rows(min, max))
            lock.synchronize {row_count[t.name] += result}
          end
        else
          row_count[t.name] = query_return_first_value(t.sql_count_rows(false, false))
        end
        output "#{row_count[t.name]} rows counted", t
      end
      row_count
    end

    # Cleans up all rows that should no longer be on this db.
    # Supply the ID range (in terms of the table's sharding key)
    # of rows to KEEP.
    def prune_data_to_range(tables, keep_min_id, keep_max_id)
      reconnect(user: app_credentials[:user])
      tables.each do |t|
        output "Cleaning up data, pruning to only keep range #{keep_min_id}-#{keep_max_id}", t
        rows_deleted = 0
        [:asc, :desc].each {|direction| rows_deleted += delete_table_data_outside_range(t, keep_min_id, keep_max_id, direction)}
        output "Done cleanup; #{rows_deleted} rows deleted", t
      end
    end

    # Helper method used by prune_data_to_range. Deletes data for the given table that falls
    # either below the supplied keep_min_id (if direction is :desc) or falls above the
    # supplied keep_max_id (if direction is :asc).
    def delete_table_data_outside_range(table, keep_min_id, keep_max_id, direction)
      rows_deleted = 0

      if direction == :asc
        dir_english = "Ascending"
        boundary = keep_max_id
        output "Removing rows with ID > #{boundary}", table
      elsif direction == :desc
        dir_english = "Descending"
        boundary = keep_min_id
        output "Removing rows with ID < #{boundary}", table
      else
        raise "Unknown order parameter #{order}"
      end

      table.sharding_keys.each do |col|
        deleter_sql = table.sql_cleanup_delete(col, keep_min_id, keep_max_id)

        id = boundary
        iter = 0
        while id do
          finder_sql = table.sql_cleanup_next_id(col, id, direction)
          id = query_return_first_value(finder_sql)
          break unless id
          rows_deleted += query(deleter_sql, id)

          # Slow down on multi-col sharding key tables, due to queries being far more expensive
          sleep(0.0001) if table.sharding_keys.size > 1

          iter += 1
          output("#{dir_english} deletion progress: through #{col} #{id}, deleted #{rows_deleted} rows so far", table) if iter % 50000 == 0
        end
      end
      rows_deleted
    end

    # Exports and re-imports data for the specified tables, optionally bounded by the
    # given range. Useful for defragmenting a node. Also useful for doing fast schema
    # alterations, if alter_schemata (or its callbacks) has been implemented.
    #
    # You can omit all params for a shard, in which case the method will use the list
    # of sharded tables in the Jetpants config file, and will use the shard's min and
    # max ID.
    def rebuild!(tables=false, min_id=false, max_id=false)
      raise "Cannot rebuild an active node" unless is_standby? || for_backups?

      p = pool
      if p.is_a?(Shard)
        tables ||= Table.from_config('sharded_tables', p.shard_pool.name)
        min_id ||= p.min_id
        max_id ||= p.max_id if p.max_id != 'INFINITY'
      end
      raise "No tables supplied" unless tables && tables.count > 0

      disable_monitoring
      stop_query_killer
      restart_mysql '--skip-log-bin', '--skip-log-slave-updates', '--innodb-autoinc-lock-mode=2', '--skip-slave-start', '--loose-gtid-mode=OFF'

      # Automatically detect missing min/max. Assumes that all tables' primary keys
      # are on the same scale, so this may be non-ideal, but better than just erroring.
      unless min_id
        tables.each do |t|
          key = t.sharding_keys[0] || t.primary_key.first
          my_min = query_return_first_value "SELECT MIN(#{key}) FROM #{t.name}"
          my_min = my_min.to_i
          min_id = my_min if !min_id || my_min < min_id
        end
      end
      unless max_id
        @found_max_ids = {} # we store the detected maxes in case DB#alter_schemata needs them later
        tables.each do |t|
          key = t.sharding_keys[0] || t.primary_key.first
          my_max = @found_max_ids[t.name] = query_return_first_value("SELECT MAX(#{key}) FROM #{t.name}")
          my_max = my_max.to_i
          max_id = my_max if !max_id || my_max > max_id
        end
      end

      export_schemata tables
      export_data tables, min_id, max_id
      
      # We need to be paranoid and confirm nothing else has restarted mysql (re-enabling binary logging)
      # out-of-band. Besides the obvious slowness of importing things while binlogging, this is outright
      # dangerous if GTID is in-use. So we check before every method or statement that does writes
      # (except for import_data, which already does its own check inside the method).
      raise "Binary logging has somehow been re-enabled. Must abort for safety!" if binary_log_enabled?
      import_schemata!
      if respond_to? :alter_schemata
        raise "Binary logging has somehow been re-enabled. Must abort for safety!" if binary_log_enabled?
        alter_schemata 
        # re-retrieve table metadata in the case that we alter the tables
        pool.probe_tables
        tables = pool.tables.select{|t| pool.tables.map(&:name).include?(t.name)}
      end

      index_list = {}
      db_prefix = "USE #{app_schema};"

      if Jetpants.import_without_indices
        tables.each do |t|
          index_list[t] = t.indexes

          t.indexes.each do |index_name, index_info|
            raise "Binary logging has somehow been re-enabled. Must abort for safety!" if binary_log_enabled?
            drop_idx_cmd = t.drop_index_query(index_name)
            output "Dropping index #{index_name} from #{t.name} prior to import"
            mysql_root_cmd("#{db_prefix}#{drop_idx_cmd}")
          end
        end
      end

      import_data tables, min_id, max_id

      if Jetpants.import_without_indices
        index_list.each do |table, indexes|
          next if indexes.keys.empty?
          raise "Binary logging has somehow been re-enabled. Must abort for safety!" if binary_log_enabled?
          create_idx_cmd = table.create_index_query(indexes)
          index_names = indexes.keys.join(", ")
          output "Recreating indexes #{index_names} for #{table.name} after import"
          mysql_root_cmd("#{db_prefix}#{create_idx_cmd}")
        end
      end

      restart_mysql
      catch_up_to_master if is_slave?
      start_query_killer
      enable_monitoring
    end

    # Copies mysql db files from self to one or more additional DBs.
    # WARNING: temporarily shuts down mysql on self, and WILL OVERWRITE CONTENTS
    # OF MYSQL DIRECTORY ON TARGETS.  Confirms first that none of the targets
    # have over 100MB of data in the schema directory or in ibdata1.
    # MySQL is restarted on source and targets afterwards.
    def clone_to!(*targets)
      targets.flatten!
      raise "Cannot clone an instance onto its master" if master && targets.include?(master)
      destinations = {}
      targets.each do |t|
        destinations[t] = t.mysql_directory
        raise "Over 100 MB of existing MySQL data on target #{t}, aborting copy!" if t.data_set_size > 100000000
      end

      # Construct the list of files and dirs to copy. We include ib_lru_dump if present
      # (ie, if using Percona Server with innodb_buffer_pool_restore_at_startup enabled)
      # since this will greatly improve warm-up time of the cloned nodes
      databases = mysql_root_cmd("SHOW DATABASES").split("\n").select { |row|
        row.include?('Database:')
      }.map{ |line|
        line.split(":").last.strip
      }.reject { |s|
        Jetpants.mysql_clone_ignore.include? s
      }
      
      # If using GTID, we need to remember the source's gtid_executed from the point-in-time of the copy
      pause_replication unless @repl_paused
      if gtid_mode?
        source_gtid_executed = gtid_executed
      end

      [self, targets].flatten.concurrent_each {|t| t.stop_query_killer; t.stop_mysql}
      targets.concurrent_each {|t| t.ssh_cmd "rm -rf #{t.mysql_directory}/ib_logfile*"}

      files = (databases + ['ibdata1', app_schema]).uniq
      files += ['*.tokudb', 'tokudb.*', 'log*.tokulog*'] if ssh_cmd("test -f #{mysql_directory}/tokudb.environment 2>/dev/null; echo $?").chomp.to_i == 0
      files << 'ib_lru_dump' if ssh_cmd("test -f #{mysql_directory}/ib_lru_dump 2>/dev/null; echo $?").chomp.to_i == 0

      fast_copy_chain(mysql_directory, destinations, :port => 3306, :files => files, :overwrite => true)
      clone_settings_to!(*targets)

      [self, targets].flatten.concurrent_each do |t|
        t.start_mysql
        t.start_query_killer
      end
      
      # If the source is using GTID, we need to set the targets' gtid_purged to equal the
      # source's gtid_executed. This is needed because we do not copy binlogs, which are
      # the source of truth for gtid_purged and gtid_executed. (Note, setting gtid_purged
      # also inherently sets gtid_executed.)
      unless source_gtid_executed.nil?
        targets.concurrent_each do |t|
          # If gtid_executed is non-empty on a fresh node, the node probably wasn't fully re-provisioned.
          # This is bad since gtid_executed is set on startup based on the binlog contents, and we can't
          # set gtid_purged unless it's empty. So we have to RESET MASTER to fix this.
          if t.gtid_executed(true) != ''
            t.output 'Node unexpectedly has non-empty gtid_executed! Probably leftover binlogs from previous life...'
            t.output 'Attempting a RESET MASTER to nuke leftovers'
            t.output t.mysql_root_cmd 'RESET MASTER'
          end
          t.gtid_purged = source_gtid_executed
          raise "Expected gtid_executed on target #{t} to now match source, but it doesn't" unless t.gtid_executed == source_gtid_executed
        end
      end
    end

    def clone_settings_to!(*targets)
      true
    end

  end
end
