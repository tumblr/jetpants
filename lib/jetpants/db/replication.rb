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
    # If you omit :user or :password, tries obtaining replication credentials from global
    # settings, and failing that from the current node (assuming it is already a slave)
    def change_master_to(new_master, option_hash={})
      return disable_replication! unless new_master   # change_master_to(nil) alias for disable_replication!
      return if new_master == master                  # no change
      
      logfile = option_hash[:log_file]
      pos     = option_hash[:log_pos]
      if !(logfile && pos)
        raise "Cannot use coordinates of a new master that is receiving updates" if new_master.master && ! new_master.repl_paused
        logfile, pos = new_master.binlog_coordinates
      end
      
      repl_user = option_hash[:user]     || Jetpants.replication_credentials[:user] || replication_credentials[:user]
      repl_pass = option_hash[:password] || Jetpants.replication_credentials[:pass] || replication_credentials[:pass]

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
      return if @repl_paused
      output "Pausing replication from #{@master}."
      output mysql_root_cmd "STOP SLAVE"
      @repl_paused = true
    end
    alias stop_replication pause_replication
    
    # Starts replication, or restarts replication after a pause
    def resume_replication
      raise "This DB object has no master" unless master
      output "Resuming replication from #{@master}."
      output mysql_root_cmd "START SLAVE"
      @repl_paused = false
    end
    alias start_replication resume_replication
    
    # Permanently disables replication
    def disable_replication!
      raise "This DB object has no master" unless master
      output "Disabling replication; this db is no longer a slave."
      output mysql_root_cmd "CHANGE MASTER TO master_host=''; STOP SLAVE; RESET SLAVE"
      @master.slaves.delete(self) rescue nil
      @master = nil
      @repl_paused = nil
    end
    alias reset_replication! disable_replication!
    
    # Wipes out the target instances and turns them into slaves of self.
    # Resumes replication on self afterwards, but does NOT automatically start
    # replication on the targets.
    # You can omit passing in the replication user/pass if this machine is itself
    # a slave OR already has at least one slave.
    # Warning: takes self offline during the process, so don't use on a master that
    # is actively in use by your application!
    def enslave!(targets, repl_user=false, repl_pass=false)
      repl_user ||= (Jetpants.replication_credentials[:user] || replication_credentials[:user])
      repl_pass ||= (Jetpants.replication_credentials[:pass] || replication_credentials[:pass])
      pause_replication if master && ! @repl_paused
      file, pos = binlog_coordinates
      clone_to!(targets)
      targets.each do |t| 
        t.change_master_to( self, 
                            log_file: file, 
                            log_pos:  pos, 
                            user:     repl_user, 
                            password: repl_pass  )
      end
      resume_replication if @master # should already have happened from the clone_to! restart anyway, but just to be explicit
    end
    
    # Wipes out the target instances and turns them into slaves of self's master.
    # Resumes replication on self afterwards, but does NOT automatically start
    # replication on the targets.
    # Warning: takes self offline during the process, so don't use on an active slave!
    def enslave_siblings!(targets)
      raise "Can only call enslave_siblings! on a slave instance" unless master
      disable_monitoring
      pause_replication unless @repl_paused
      file, pos = repl_binlog_coordinates
      clone_to!(targets)
      targets.each do |t| 
        t.change_master_to( master, 
                            log_file: file,
                            log_pos:  pos,
                            user:     (Jetpants.replication_credentials[:user] || replication_credentials[:user]),
                            password: (Jetpants.replication_credentials[:pass] || replication_credentials[:pass])  )
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
      slave_status[:seconds_behind_master].to_i
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
    # propagates the info back to the Jetpants singleton, and returns it
    def replication_credentials
      unless @master || @slaves.count > 0
        raise "Cannot obtain replication credentials from orphaned instance -- this instance is not a slave, and has no slaves"
      end
      target = (@master ? self : @slaves[0])
      user, pass = target.ssh_cmd("cat #{mysql_directory}/master.info | head -6 | tail -2").split
      {user: user, pass: pass}
    end
    
    # Disables binary logging in my.cnf.  Does not take effect until you restart
    # mysql.
    def disable_binary_logging
      output "Disabling binary logging in MySQL configuration; will take effect at next restart"
      comment_out_ini(mysql_config_file, 'log-bin', 'log-slave-updates')
    end
    
    # Re-enables binary logging in my.cnf after a prior call to disable_bin_log.
    # Does not take effect until you restart mysql.
    def enable_binary_logging
      output "Re-enabling binary logging in MySQL configuration; will take effect at next restart"
      uncomment_out_ini(mysql_config_file, 'log-bin', 'log-slave-updates')
    end
    
  end
end