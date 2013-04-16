module Jetpants
  
  #--
  # MySQL server manipulation methods ##########################################
  #++
  
  class DB
    # Shuts down MySQL, and confirms that it is no longer listening.
    # OK to use this if MySQL is already stopped; it's a no-op then.
    def stop_mysql
      output "Attempting to shutdown MySQL"
      disconnect if @db
      output service(:stop, 'mysql')
      running = ssh_cmd "netstat -ln | grep ':#{@port}' | wc -l"
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
      running = ssh_cmd "netstat -ln | grep ':#{@port}' | wc -l"
      raise "[#{@ip}] Failed to start MySQL: Something is already listening on port #{@port}" unless running.chomp == '0'
      if options.size == 0
        output "Attempting to start MySQL, no option overrides supplied"
      else
        output "Attempting to start MySQL with options #{options.join(' ')}"
      end
      output service(:start, 'mysql', options.join(' '))
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
      '/var/lib/mysql'
    end
    
    # Has no built-in effect. Plugins can override it, and/or implement
    # before_enable_monitoring and after_enable_monitoring callbacks.
    def enable_monitoring(*services)
    end
    
    # Has no built-in effect. Plugins can override it, and/or implement
    # before_disable_monitoring and after_disable_monitoring callbacks.
    def disable_monitoring(*services)
    end
    
  end
end
