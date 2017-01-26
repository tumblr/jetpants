module Jetpants
  
  #--
  # MySQL server manipulation methods ##########################################
  #++
  
  class DB

    # options to pass to MySQL on start
    attr_reader :start_options

    # options to pass to DB#restart_mysql for quick restarts
    attr_accessor :enable_flush_innodb_cache

    # add a server start option for the instance
    # will be combined with options passed into start_mysql
    def add_start_option(option)
      @start_options ||= []
      @start_options << option unless @start_options.include? option
    end

    # remove a start option from the db instance
    # will not prevent the option from being passed into
    # start_mysql
    def remove_start_option(option)
      @start_options ||= []
      @start_options.delete option
    end

    # Shuts down MySQL, and confirms that it is no longer listening.
    # OK to use this if MySQL is already stopped; it's a no-op then.
    def stop_mysql
      output "Attempting to shutdown MySQL"
      
      # Ensure GTID-related variables persist across a planned restart. This is needed regardless
      # of any plugins rewriting my.cnf based on pool membership, since there are scenarios
      # involving new nodes needing correct gtid_mode *prior* to Pool#sync_configuration being
      # called. (Note: DB#start_mysql is smart enough to *temporarily* ignore gtid_mode if
      # specifically starting with binlogging disabled.)
      if gtid_mode?
        add_start_option '--loose-gtid-mode=ON'
        add_start_option '--enforce-gtid-consistency=1'
      end
      if gtid_deployment_step?
        add_start_option '--loose-gtid-deployment-step=1'
      end
      
      disconnect if @db
      output service(:stop, 'mysql')
      running = ssh_cmd "netstat -ln | grep \":#{@port}\\s\" | wc -l"
      raise "[#{@ip}] Failed to shut down MySQL: Something is still listening on port #{@port}" unless running.chomp == '0'
      @options = []
      @running = false
    end
    
    # Starts MySQL, and confirms that something is now listening on the port.
    # Raises an exception if MySQL is already running or if something else is
    # already running on its port.
    # Options should be supplied as positional method args, for example:
    #   start_mysql '--skip-networking', '--skip-grant-tables'
    def start_mysql(*options)
      if @master
        @repl_paused = options.include?('--skip-slave-start')
      end
      mysql_start_options = [ options, start_options ].flatten
      mysql_start_options.delete '--loose-gtid-mode=ON' if mysql_start_options.include? '--skip-log-bin'
      running = ssh_cmd "netstat -ln | grep ':#{@port}' | wc -l"
      raise "[#{@ip}] Failed to start MySQL: Something is already listening on port #{@port}" unless running.chomp == '0'
      if mysql_start_options.size == 0
        output "Attempting to start MySQL, no option overrides supplied"
      else
        output "Attempting to start MySQL with options #{mysql_start_options.join(' ')}"
      end
      output service(:start, 'mysql', mysql_start_options.join(' '))
      @options = options
      confirm_listening
      @running = true
      if role == :master && ! @options.include?('--skip-networking')
        disable_read_only!
      end
    end
    
    # Restarts MySQL.
    def restart_mysql(*options)
      if @master
        @repl_paused = options.include?('--skip-slave-start')
      end
      
      # Ensure that GTID-related variables persist across a planned restart. This is needed regardless
      # of any Pool#rewrite_options_files_for_gtid_rollout implementation since there are scenarios
      # involving new nodes needing correct gtid_mode *prior* to Pool#sync_configuration being
      # called.
      if gtid_mode? && !options.include?('--skip-log-bin')
        options << '--loose-gtid-mode=ON' << '--enforce-gtid-consistency=1'
      end
      if gtid_deployment_step?
        options << '--loose-gtid-deployment-step=1'
      end
      
      # DB#Restart_mysql has the ability to do a fast restart using mechanism specified at
      # https://www.percona.com/blog/2009/04/15/how-to-decrease-innodb-shutdown-times/
      # If called with an argument to DB#Restart_mysql, the function will use innodb_flush_iops
      # config to set innodb_io_capacity to the value. This speeds up the flushing process.
      if @enable_flush_innodb_cache
         flush_innodb_cache
      end

      # Disconnect if we were previously connected
      user, schema = false, false
      if @db
        user, schema = @user, @schema
        disconnect
      end
      
      if options.size == 0
        output "Attempting to restart MySQL, no option overrides supplied"
      else
        output "Attempting to restart MySQL with options #{options.join(' ')}"
      end
      output service(:restart, 'mysql', options.join(' '))
      @options = options
      confirm_listening
      @running = true
      unless @options.include?('--skip-networking')
        disable_read_only! if role == :master
        
        # Reconnect if we were previously connected
        connect(user: user, schema: schema) if user || schema
      end
    end

    # DB#Flush_innodb_cache function flushes the dirty pages from Innodb
    # buffer pool aggressively. This function should mostly be used in
    # conjunction with DB#Restart_mysql where, flushing pages prior to 
    # restart reduces the time require to shutdown MySQL there-by reducing
    # the restart times.
    def flush_innodb_cache(timeout=1800, poll_frequency=30)
      # Before setting any variables we collect their current values to reset later on.
      prev_innodb_max_dirty_pages_pct = global_variables[:innodb_max_dirty_pages_pct].to_i
      prev_innodb_io_capacity_max = global_variables[:innodb_io_capacity_max].to_i
      prev_innodb_io_capacity = global_variables[:innodb_io_capacity].to_i
      # innodb_io_capacity can improve the performance of dirty page flushing in case faster storage is available, for eg on a flash storage increasing innodb_io_capacity to 50000 can result in innodb flushing more aggressively.
      io_capacity = Jetpants.innodb_flush_iops.to_i
      set_vars = "set global innodb_max_dirty_pages_pct = 0, global innodb_io_capacity_max = #{io_capacity + 2000}, global innodb_io_capacity =  #{io_capacity}"
      mysql_root_cmd(set_vars)

      total_bufferpool_pages = global_status[:Innodb_buffer_pool_pages_data].to_i
      reset_vars = "set global innodb_io_capacity = #{prev_innodb_io_capacity}, global innodb_io_capacity_max = #{prev_innodb_io_capacity_max}, global innodb_max_dirty_pages_pct = #{prev_innodb_max_dirty_pages_pct}"
      start = Time.now.to_i
      output "Starting to flush dirty buffers to disk"
      while (Time.now.to_i - start) < timeout
        pages = global_status[:Innodb_buffer_pool_pages_dirty].to_i
        if pages < (total_bufferpool_pages/100) * 1.5
          output "Dirty buffers have been flushed to disk, only 1.5% remaining."
          mysql_root_cmd(reset_vars)
          return true
        else
          output "Number of dirty pages remaining to be flushed: #{pages}"
          sleep poll_frequency
        end
      end
      raise "This instance was not able to flush all the dirty buffers within #{timeout} seconds. Resetting themysql variables back to previous values."
      mysql_root_cmd(reset_vars)
    end
    
    # Has no built-in effect. Plugins can override it, and/or implement
    # before_stop_query_killer and after_stop_query_killer callbacks.
    def stop_query_killer
    end
    
    # Has no built-in effect. Plugins can override it, and/or implement
    # before_start_query_killer and after_start_query_killer callbacks.
    def start_query_killer
    end
    
    # Confirms that a process is listening on the DB's port
    def confirm_listening(timeout=10)
      if @options.include? '--skip-networking'
        output 'Unable to confirm mysqld listening because server started with --skip-networking'
        false
      else
        confirm_listening_on_port(@port, timeout)
      end
    end
    
    # Returns the MySQL data directory for this instance. A plugin can override this
    # if needed, especially if running multiple MySQL instances on the same host.
    def mysql_directory
      Jetpants.mysql_datadir
    end
    
    # Has no built-in effect. Plugins can override it, and/or implement
    # before_enable_monitoring and after_enable_monitoring callbacks.
    def enable_monitoring(*services)
    end
    
    # Has no built-in effect. Plugins can override it, and/or implement
    # before_disable_monitoring and after_disable_monitoring callbacks.
    def disable_monitoring(*services)
    end
    
    # No built-in effect. Use when performing actions which will cause the
    # server to go offline or become unresponsive, as an escalated enable_monitoring
    # Plugins can override and/or implement before/after hooks
    def set_downtime(hours)
    end

    # No built-in effect. Use when performing actions which will cause the
    # server to go offline or become unresponsive, as an escalated disable_monitoring
    # Plugins can override and/or implement before/after hooks
    def cancel_downtime
    end

    # Run tcpdump on the MySQL traffic and return the top 30 slowest queries
    def get_query_runtime(duration, database = nil)
      raise 'Percona::Toolkit is not installed on the server' if self.ssh_cmd('which pt-query-digest 2> /dev/null').nil?

      dumpfile = File.join(Dir.tmpdir, 'jetpants_tcpdump.' + (0...8).map { (65 + rand(26)).chr }.join)
      get_tcpdump_sample duration, dumpfile

      if database
        output("Analyzing the tcpdump with pt-query-digest for database '#{database}'")
        pt_query_digest = "pt-query-digest --filter '$event->{db} && $event->{db} eq \"#{database}\"' --type tcpdump --limit 30 - 2> /dev/null"
      else
        output('Analyzing the tcpdump with pt-query-digest')
        pt_query_digest = 'pt-query-digest --type tcpdump --limit 30 - 2> /dev/null'
      end

      output(self.ssh_cmd "tcpdump -s 0 -x -n -q -tttt -r #{dumpfile} | #{pt_query_digest}")
      self.ssh_cmd "rm -f #{dumpfile}"

      nil
    end

    def get_tcpdump_sample(duration, dumpfile)
      raise 'tcpdump is not installed on the server' if self.ssh_cmd('which tcpdump 2> /dev/null').nil?

      output("Running tcpdump for #{duration} seconds and dumping temp data to #{dumpfile}")
      self.ssh_cmd "tcpdump -i #{Jetpants.private_interface} -G #{duration} -W 1 'port #{@port}' -w #{dumpfile}"
    end
  end
end
