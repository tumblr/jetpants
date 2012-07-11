module Jetpants
  
  #--
  # MySQL server manipulation methods ##########################################
  #++
  
  class DB
    # Shuts down MySQL, and confirms that it is no longer listening.
    # OK to use this if MySQL is already stopped; it's a no-op then.
    def stop_mysql
      output "Attempting to shutdown MySQL"
      output service(:stop, 'mysql')
      running = ssh_cmd "netstat -ln | grep #{@port} | wc -l"
      raise "[#{@ip}] Failed to shut down MySQL: Something is still listening on port #{@port}" unless running.chomp == '0'
      @running = false
    end
    
    # Starts MySQL, and confirms that something is now listening on the port.
    # Raises an exception if MySQL is already running or if something else is
    # already running on its port.
    def start_mysql
      @repl_paused = false if @master
      running = ssh_cmd "netstat -ln | grep #{@port} | wc -l"
      raise "[#{@ip}] Failed to start MySQL: Something is already listening on port #{@port}" unless running.chomp == '0'
      output "Attempting to start MySQL"
      output service(:start, 'mysql')
      confirm_listening
      @running = true
    end
    
    # Restarts MySQL.
    def restart_mysql
      @repl_paused = false if @master
      
      # Disconnect if we were previously connected
      user, schema = false, false
      if @db
        user, schema = @user, @schema
        disconnect
      end
      
      output "Attempting to restart MySQL"
      output service(:restart, 'mysql')
      confirm_listening
      @running = true
      
      # Reconnect if we were previously connected
      connect(user: user, schema: schema) if user || schema
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
      confirm_listening_on_port(@port, timeout)
    end
    
    # Returns the MySQL data directory for this instance. A plugin can override this
    # if needed, especially if running multiple MySQL instances on the same host.
    def mysql_directory
      '/var/lib/mysql'
    end
    
    # Returns the MySQL server configuration file for this instance. A plugin can
    # override this if needed, especially if running multiple MySQL instances on
    # the same host.
    def mysql_config_file
      '/etc/my.cnf'
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
