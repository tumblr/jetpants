module Jetpants
  
  #--
  # Replication and binlog-related methods #####################################
  #++
  
  class DB
    # Changes the master for this instance. Supply a Jetpants::DB indicating the new
    # master, along with options :log_pos, :log_file, :user, :password.
    # Does NOT automatically start replication afterwards on self!
    #
    # If you omit :log_pos or :log_file, uses the current position/file from new_master,
    # though this is only safe if new_master is not receiving writes!
    #
    # If you omit :user and :password, tries obtaining replication credentials from the
    # current node (assuming it is already a slave) or if that fails then from the global
    # settings.
    def change_master_to(new_master, option_hash={})
      return disable_replication! unless new_master   # change_master_to(nil) alias for disable_replication!
      return if new_master == master                  # no change
      
      logfile = option_hash[:log_file]
      pos     = option_hash[:log_pos]
      if !(logfile && pos)
        raise "Cannot use coordinates of a new master that is receiving updates" if new_master.master && ! new_master.repl_paused?
        logfile, pos = new_master.binlog_coordinates
      end
      
      repl_user = option_hash[:user]     || replication_credentials[:user]
      repl_pass = option_hash[:password] || replication_credentials[:pass]

      pause_replication if @master && !@repl_paused
      result = mysql_root_cmd "CHANGE MASTER TO " +
        "MASTER_HOST='#{new_master.ip}', " +
        "MASTER_PORT=#{new_master.port}, " +
        "MASTER_LOG_FILE='#{logfile}', " +
        "MASTER_LOG_POS=#{pos}, " +
        "MASTER_USER='#{repl_user}', " + 
        "MASTER_PASSWORD='#{repl_pass}'"
      
      output "Changing master to #{new_master} with coordinates (#{logfile}, #{pos}). #{result}"
      @master.slaves.delete(self) if @master rescue nil
      @master = new_master
      @repl_paused = true
      new_master.slaves << self
    end
    
    # Pauses replication
    def pause_replication
      raise "This DB object has no master" unless master
      output "Pausing replication from #{@master}."
      if @repl_paused
        output "Replication was already paused."
        repl_binlog_coordinates(true)
      else
        output mysql_root_cmd "STOP SLAVE"
        repl_binlog_coordinates(true)
        @repl_paused = true
      end
    end
    alias stop_replication pause_replication
    
    # Starts replication, or restarts replication after a pause
    def resume_replication
      raise "This DB object has no master" unless master
      repl_binlog_coordinates(true)
      output "Resuming replication from #{@master}."
      output mysql_root_cmd "START SLAVE"
      @repl_paused = false
    end
    alias start_replication resume_replication
    
    # Stops replication at the same coordinates on two nodes
    def pause_replication_with(sibling)
      [self, sibling].each &:pause_replication
      
      # self and sibling at same coordinates: all done
      return true if repl_binlog_coordinates == sibling.repl_binlog_coordinates
      
      # self ahead of sibling: handle via recursion with roles swapped
      return sibling.pause_replication_with(self) if ahead_of? sibling
      
      # sibling ahead of self: catch up to sibling
      sibling_coords = sibling.repl_binlog_coordinates
      output "Resuming replication from #{@master} until (#{sibling_coords[0]}, #{sibling_coords[1]})."
      output(mysql_root_cmd "START SLAVE UNTIL MASTER_LOG_FILE = '#{sibling_coords[0]}', MASTER_LOG_POS = #{sibling_coords[1]}")
      sleep 1 while repl_binlog_coordinates != sibling_coords
      true
    end
    
    # Permanently disables replication. Clears out the SHOW SLAVE STATUS output
    # entirely in MySQL versions that permit this.
    def disable_replication!
      stop_replication
      output "Disabling replication; this db is no longer a slave."
      ver = version_tuple
      
      # MySQL < 5.5: allows master_host='', which clears out SHOW SLAVE STATUS
      if ver[0] == 5 && ver[1] < 5
        output mysql_root_cmd "CHANGE MASTER TO master_host=''; RESET SLAVE"
      
      # MySQL 5.5.16+: allows RESET SLAVE ALL, which clears out SHOW SLAVE STATUS
      elsif ver[0] >= 5 && (ver[0] > 5 || ver[1] >= 5) && (ver[0] > 5 || ver[1] > 5 || ver[2] >= 16)
        output mysql_root_cmd "CHANGE MASTER TO master_user='test'; RESET SLAVE ALL"
      
      # Other versions: no safe way to clear out SHOW SLAVE STATUS.  Still set master_user to 'test'
      # so that we know to ignore the slave status output.
      else
        output mysql_root_cmd "CHANGE MASTER TO master_user='test'; RESET SLAVE"
      end
      
      @master.slaves.delete(self) rescue nil
      @master = nil
      @repl_paused = nil
    end
    alias reset_replication! disable_replication!
    
    # Wipes out the target instances and turns them into slaves of self.
    # Resumes replication on self afterwards, but does NOT automatically start
    # replication on the targets.
    # You can omit passing in the replication user/pass if this machine is itself
    # a slave OR already has at least one slave OR the global setting is fine to use here.
    # Warning: takes self offline during the process, so don't use on a master that
    # is actively in use by your application!
    def enslave!(targets, repl_user=false, repl_pass=false)
      repl_user ||= replication_credentials[:user]
      repl_pass ||= replication_credentials[:pass]
      disable_monitoring
      targets.each {|t| t.disable_monitoring}
      pause_replication if master && ! @repl_paused
      file, pos = binlog_coordinates
      clone_to!(targets)
      targets.each do |t|
        t.enable_monitoring
        t.change_master_to( self, 
                            log_file: file, 
                            log_pos:  pos, 
                            user:     repl_user, 
                            password: repl_pass  )
        t.enable_read_only!
      end
      resume_replication if @master # should already have happened from the clone_to! restart anyway, but just to be explicit
      enable_monitoring
    end
    
    # Wipes out the target instances and turns them into slaves of self's master.
    # Resumes replication on self afterwards, but does NOT automatically start
    # replication on the targets.
    # Warning: takes self offline during the process, so don't use on an active slave!
    def enslave_siblings!(targets)
      raise "Can only call enslave_siblings! on a slave instance" unless master
      disable_monitoring
      targets.each {|t| t.disable_monitoring}
      pause_replication unless @repl_paused
      file, pos = repl_binlog_coordinates
      clone_to!(targets)
      targets.each do |t| 
        t.enable_monitoring
        t.change_master_to( master, 
                            log_file: file,
                            log_pos:  pos,
                            user:     replication_credentials[:user],
                            password: replication_credentials[:pass]  )
        t.enable_read_only!
      end
      resume_replication # should already have happened from the clone_to! restart anyway, but just to be explicit
      catch_up_to_master
      enable_monitoring
    end
    
    # Shortcut to call DB#enslave_siblings! on a single target
    def enslave_sibling!(target)
      enslave_siblings!([target])
    end
    
    # Use this on a slave to return [master log file name, position] for how far
    # this slave has executed (in terms of its master's binlogs) in its SQL replication thread.
    def repl_binlog_coordinates(display_info=true)
      raise "This instance is not a slave" unless master
      status = slave_status
      file, pos = status[:relay_master_log_file], status[:exec_master_log_pos].to_i
      output "Has executed through master's binlog coordinates of (#{file}, #{pos})." if display_info
      [file, pos]
    end
    
    # Returns a two-element array containing [log file name, position] for this
    # database. Only useful when called on a master. This is the current
    # instance's own binlog coordinates, NOT the coordinates of replication
    # progress on a slave!
    def binlog_coordinates(display_info=true)
      hash = mysql_root_cmd('SHOW MASTER STATUS', :parse=>true)
      raise "Cannot obtain binlog coordinates of this master becaues binary logging is not enabled" unless hash[:file]
      output "Own binlog coordinates are (#{hash[:file]}, #{hash[:position].to_i})." if display_info
      [hash[:file], hash[:position].to_i]
    end
    
    # Returns the number of seconds beind the master the replication execution is,
    # as reported by SHOW SLAVE STATUS.
    def seconds_behind_master
      raise "This instance is not a slave" unless master
      lag = slave_status[:seconds_behind_master]
      lag == 'NULL' ? nil : lag.to_i
    end
    
    # Waits for this instance's SECONDS_BEHIND_MASTER to reach 0 and stay at
    # 0 after repeated polls (based on threshold and poll_frequency).  Will raise
    # an exception if this has not happened within the timeout period, in seconds.
    # In other words, with default settings: checks slave lag every 5+ sec, and
    # returns true if slave lag is zero 3 times in a row. Gives up if this does
    # not occur within a one-hour period. If a large amount of slave lag is
    # reported, this method will automatically reduce its polling frequency.
    def catch_up_to_master(timeout=3600, threshold=3, poll_frequency=5)
      raise "This instance is not a slave" unless master
      resume_replication if @repl_paused
      
      times_at_zero = 0
      start = Time.now.to_i
      output "Waiting to catch up to master"
      while (Time.now.to_i - start) < timeout
        lag = seconds_behind_master
        if lag == 0
          times_at_zero += 1
          if times_at_zero >= threshold
            output "Caught up to master."
            return true
          end
          sleep poll_frequency
        elsif lag.nil?
          resume_replication
          sleep 1
          raise "Unable to restart replication" if seconds_behind_master.nil?
        else
          output "Currently #{lag} seconds behind master."
          times_at_zero = 0
          extra_sleep_time = (lag > 30000 ? 300 : (seconds_behind_master / 100).ceil)
          sleep poll_frequency + extra_sleep_time
        end
      end
      raise "This instance did not catch up to its master within #{timeout} seconds"
    end
    
    # Returns a hash containing the information from SHOW SLAVE STATUS
    def slave_status
      hash = mysql_root_cmd('SHOW SLAVE STATUS', :parse=>true)
      hash = {} if hash[:master_user] == 'test'
      if @master && hash.count < 1
        message = "should be a slave of #{@master}, but SHOW SLAVE STATUS indicates otherwise"
        raise "#{self}: #{message}" if Jetpants.verify_replication
        output message
      end
      hash
    end
    
    # Reads an existing master.info file on this db or one of its slaves,
    # propagates the info back to the Jetpants singleton, and returns it as
    # a hash containing :user and :pass.
    # If the node is not a slave and has no slaves, will use the global Jetpants
    # config instead.
    def replication_credentials
      user = false
      pass = false
      if master || slaves.count > 0
        target = (@master ? self : @slaves[0])
        results = target.ssh_cmd("cat #{mysql_directory}/master.info | head -6 | tail -2").split
        if results.count == 2 && results[0] != 'test'
          user, pass = results
        end
      end
      user && pass ? {user: user, pass: pass} : Jetpants.replication_credentials
    end
    
    # This method is no longer supported. It used to manipulate /etc/my.cnf directly, which was too brittle.
    # You can achieve the same effect by passing parameters to DB#restart_mysql.
    def disable_binary_logging
      raise "DB#disable_binary_logging is no longer supported, please use DB#restart_mysql('--skip-log-bin', '--skip-log-slave-updates') instead"
    end
    
    # This method is no longer supported. It used to manipulate /etc/my.cnf directly, which was too brittle.
    # You can achieve the same effect by passing (or NOT passing) parameters to DB#restart_mysql.
    def enable_binary_logging
      raise "DB#enable_binary_logging is no longer supported, please use DB#restart_mysql() instead"
    end
    
    # Return true if this node's replication progress is ahead of the provided
    # node, or false otherwise. The nodes must be in the same pool for coordinates
    # to be comparable. Does not work in hierarchical replication scenarios!
    def ahead_of?(node)
      my_pool = pool(true)
      raise "Node #{node} is not in the same pool as #{self}" unless node.pool(true) == my_pool
      
      my_coords   = (my_pool.master == self ? binlog_coordinates      : repl_binlog_coordinates)
      node_coords = (my_pool.master == node ? node.binlog_coordinates : node.repl_binlog_coordinates)
      
      # Same coordinates
      if my_coords == node_coords
        false
      
      # Same logfile: simply compare position
      elsif my_coords[0] == node_coords[0]
        my_coords[1] > node_coords[1]
        
      # Different logfile
      else
        my_logfile_num = my_coords[0].match(/^[a-zA-Z.0]+(\d+)$/)[1].to_i
        node_logfile_num = node_coords[0].match(/^[a-zA-Z.0]+(\d+)$/)[1].to_i
        my_logfile_num > node_logfile_num
      end
    end
    
  end
end