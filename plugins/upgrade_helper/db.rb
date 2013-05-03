module Jetpants
  class DB
    attr_accessor :needs_upgrade
    
    ##### CALLBACKS ############################################################
    
    # Handle upgrading mysql if needed
    def before_start_mysql(*options)
      return unless @needs_upgrade
      
      @repl_paused = false if @master
      running = ssh_cmd "netstat -ln | grep #{@port} | wc -l"
      raise "[#{@ip}] Failed to start MySQL: Something is already listening on port #{@port}" unless running.chomp == '0'
        
      output "Attempting to start MySQL with --skip-networking --skip-grant-tables in prep for upgrade"
      
      # Can't use start_mysql here without causing infinite recursion! Also don't need
      # to do all the same checks here, nor do we need to store these to @options.
      output service(:start, 'mysql', '--skip-networking --skip-grant-tables')
      
      output "Attempting to run mysql_upgrade"
      output ssh_cmd('mysql_upgrade')
      
      output "Upgrade complete"
      @needs_upgrade = false
      
      # Now shut down mysql, so that start_mysql can restart it without the --skip-* options
      stop_mysql
    end
    
    ##### NEW METHODS ##########################################################
    
    # Creates a temporary user for use of pt-table-checksum, yields to the 
    # supplied block, and then drops the user.
    # The user will have a randomly-generated 50-character password, and will
    # have elevated permissions (ALL PRIVILEGES on the application schema, and
    # a few global privs as well) since these are necessary to run the tools.
    # The block will be passed the randomly-generated password.
    def with_pt_checksum_user(username='pt-checksum')
      password = DB.random_password
      create_user username, password
      grant_privileges username, '*', 'PROCESS', 'REPLICATION CLIENT',  'REPLICATION SLAVE'
      grant_privileges username, app_schema, 'ALL PRIVILEGES'
      begin
        yield username, password
      rescue
        drop_user username
        raise
      end
      drop_user username
    end
    
    # Captures mysql traffic with tcpdump for the specified amount of time, in seconds.
    # The dumpfile will be saved to #{Jetpants.export_location} with filename 
    # #{hostname}.dumpfile, and the filename portion will be returned by this method.
    #
    # Not all traffic will be included -- uses a method by Devananda van der Veen described in 
    # http://www.mysqlperformanceblog.com/2011/04/18/how-to-use-tcpdump-on-very-busy-hosts/
    # to sample the traffic.
    #
    # Requires that tcpdump is available in root's PATH. Also assumes root's shell is bash
    # or supports equivalent syntax. Currently only works if mysqld running on port 3306.
    #
    # Warning: tcpdump can be taxing on the server, and also can generate rather large
    # amounts of output! Also, will overwrite any previous file at the destination path!
    def tcpdump!(duration=30, interface=false)
      interface ||= Jetpants.private_interface
      output "Using tcpdump to capture sample of MySQL traffic for #{duration} seconds"
      tcpdump_options = "-i #{interface} -s 65535 -x -n -q -tttt 'port 3306 and tcp[1] & 7 == 2 and tcp[3] & 7 == 2'"
      outfile = "#{Jetpants.export_location}/#{hostname}.dumpfile"
      ssh_cmd "tcpdump #{tcpdump_options} > #{outfile} & export DUMP_PID=$! && sleep #{duration} && kill $DUMP_PID"
      output "Completed capturing traffic sample"
      "#{hostname}.dumpfile"
    end
    
    
  end
end