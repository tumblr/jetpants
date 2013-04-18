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
    def export_data(tables, min_id=false, max_id=false)
      pause_replication if @master && ! @repl_paused
      import_export_user = 'jetpants'
      create_user(import_export_user)
      grant_privileges(import_export_user)               # standard privs
      grant_privileges(import_export_user, '*', 'FILE')  # FILE global privs
      reconnect(user: import_export_user)
      @counts ||= {}
      tables.each {|t| @counts[t.name] = export_table_data t, min_id, max_id}
    ensure
      reconnect(user: app_credentials[:user])
      drop_user import_export_user
    end
    
    # Exports data for a table. Only includes the data subset that falls
    # within min_id and max_id. The export files will be located according
    # to the export_location configuration setting.
    # Returns the number of rows exported.
    def export_table_data(table, min_id=false, max_id=false)
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
      output "#{rows_exported} rows exported", table
      rows_exported
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
    # Note: import will be substantially faster if you disable binary logging
    # before the import, and re-enable it after the import. You also must set
    # InnoDB's autoinc lock mode to 2 in order to do a chunked import with
    # auto-increment tables.  You can achieve all this by calling
    # DB#restart_mysql '--skip-log-bin', '--skip-log-slave-updates', '--innodb-autoinc-lock-mode=2'
    # prior to importing data, and then clear those settings by calling
    # DB#restart_mysql with no params after done importing data.
    def import_data(tables, min_id=false, max_id=false)
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
      tables.each {|t| import_counts[t.name] = import_table_data t, min_id, max_id}
      
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
    
    # Imports the data subset previously dumped thorugh export_data.
    # Returns number of rows imported.
    def import_table_data(table, min_id=false, max_id=false)
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
          sql = table.sql_import_range(min, max)
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
        tables ||= Table.from_config 'sharded_tables'
        min_id ||= p.min_id
        max_id ||= p.max_id if p.max_id != 'INFINITY'
      end
      raise "No tables supplied" unless tables && tables.count > 0
      
      disable_monitoring
      stop_query_killer
      restart_mysql '--skip-log-bin', '--skip-log-slave-updates', '--innodb-autoinc-lock-mode=2', '--skip-slave-start'
      
      # Automatically detect missing min/max. Assumes that all tables' primary keys
      # are on the same scale, so this may be non-ideal, but better than just erroring.
      unless min_id
        tables.each do |t|
          my_min = query_return_first_value "SELECT MIN(#{t.sharding_keys[0]}) FROM #{t.name}"
          min_id = my_min if !min_id || my_min < min_id
        end
      end
      unless max_id
        @found_max_ids = {} # we store the detected maxes in case DB#alter_schemata needs them later
        tables.each do |t|
          my_max = @found_max_ids[t.name] = query_return_first_value("SELECT MAX(#{t.sharding_keys[0]}) FROM #{t.name}")
          max_id = my_max if !max_id || my_max > max_id
        end
      end
      
      export_schemata tables
      export_data tables, min_id, max_id
      import_schemata!
      alter_schemata if respond_to? :alter_schemata
      import_data tables, min_id, max_id
      
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
      [self, targets].flatten.concurrent_each {|t| t.stop_query_killer; t.stop_mysql}
      targets.concurrent_each {|t| t.ssh_cmd "rm -rf #{t.mysql_directory}/ib_logfile*"}
      
      # Construct the list of files and dirs to copy. We include ib_lru_dump if present
      # (ie, if using Percona Server with innodb_buffer_pool_restore_at_startup enabled)
      # since this will greatly improve warm-up time of the cloned nodes
      files = ['ibdata1', 'mysql', 'test', app_schema]
      files << 'ib_lru_dump' if ssh_cmd("test -f #{mysql_directory}/ib_lru_dump 2>/dev/null; echo $?").chomp.to_i == 0
      
      fast_copy_chain(mysql_directory, 
                      destinations,
                      port: 3306,
                      files: files,
                      overwrite: true)
      [self, targets].flatten.concurrent_each {|t| t.start_mysql; t.start_query_killer}
    end
    
  end
end