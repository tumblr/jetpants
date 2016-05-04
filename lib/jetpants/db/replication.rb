module Jetpants
  
  #--
  # Replication and binlog-related methods #####################################
  #++
  
  class DB
    # Changes the master for this instance. Does NOT automatically start
    # replication afterwards on self!
    #
    # Supply a Jetpants::DB indicating the new master, along with any options:
    #   :user          -- replication username
    #   :password      -- replication password corresponding to :user
    #   :log_file      -- master binlog filename, if not using :auto_position
    #   :log_pos       -- master binlog position, if not using :auto_position
    #   :auto_position -- use GTID auto-positioning instead of binlog coords (boolean, NOT int!)
    #   :ssl_ca_path
    #   :ssl_client_cert_path
    #   :ssl_client_key_path
    #
    # If you omit :log_pos or :log_file AND omit :auto_position, behavior depends on
    # whether GTID enabled. If enabled, uses auto-positioning anyway; otherwise uses
    # the current position/file from new_master. The latter is only safe if new_master
    # is not receiving writes!
    #
    # If you omit :user and :password, tries obtaining replication credentials from the
    # current node (assuming it is already a slave) or if that fails then from the global
    # settings.
    def change_master_to(new_master, option_hash={})
      return disable_replication! unless new_master   # change_master_to(nil) alias for disable_replication!
      return if new_master == master                  # no change
      
      # Prevent trivial replication circles, i.e. a->b->a or the nonsensical a->a.
      # This isn't a comprehensive check since it doesn't catch 3+ node rings, but
      # at least this simple check will prevent human error.
      raise "Circular replication not supported" if new_master.master == self || new_master == self
      
      # Either BOTH log_file and log_pos must be supplied, OR neither supplied
      raise "log_file and log_pos options must be supplied together" if (option_hash[:log_file] && !option_hash[:log_pos]) || (option_hash[:log_pos] && !option_hash[:log_file])
      
      # If no valid coords nor auto-position supplied, default behavior depends on
      # whether or not the pool is using GTID
      unless (option_hash[:log_file] && option_hash[:log_pos]) || option_hash[:auto_position]
        if pool(true).gtid_mode?
          option_hash[:auto_position] = true
        else
          raise "Cannot use default coordinates of a new master that is receiving updates without GTID" if new_master.taking_writes?(0.5)
          option_hash[:log_file], option_hash[:log_pos] = new_master.binlog_coordinates
        end
      end
      
      if option_hash[:auto_position]
        raise "When using auto-positioning, do not supply coordinates to DB#change_master_to" if option_hash[:log_file] || option_hash[:log_pos]
        raise "Auto-positioning requires master and replica to be using gtid_mode" unless gtid_mode? && new_master.gtid_mode?
        position_clause = "MASTER_AUTO_POSITION = 1, "
      else
        position_clause = "MASTER_LOG_FILE='#{option_hash[:log_file]}', MASTER_LOG_POS=#{option_hash[:log_pos]}, "
        if pool(true).gtid_mode?
          output "WARNING: using coordinates in CHANGE MASTER despite GTID being available in this pool"
          position_clause += "MASTER_AUTO_POSITION = 0, " # necessary to work if auto-positioning was previously 1
        end
      end
      
      repl_user = option_hash[:user]     || replication_credentials[:user]
      repl_pass = option_hash[:password] || replication_credentials[:pass]
      use_ssl   = new_master.use_ssl_replication? && use_ssl_replication?

      pause_replication if @master && !@repl_paused
      cmd_str = "CHANGE MASTER TO " +
        "MASTER_HOST='#{new_master.ip}', " +
        "MASTER_PORT=#{new_master.port}, " +
        position_clause +
        "MASTER_USER='#{repl_user}', " + 
        "MASTER_PASSWORD='#{repl_pass}'"

      if use_ssl
        ssl_ca_path = option_hash[:ssl_ca_path] || Jetpants.ssl_ca_path
        ssl_client_cert_path = option_hash[:ssl_client_cert_path] || Jetpants.ssl_client_cert_path
        ssl_client_key_path = option_hash[:ssl_client_key_path] || Jetpants.ssl_client_key_path
        ssl_master_cipher = option_hash[:ssl_master_cipher] || Jetpants.ssl_master_cipher

        cmd_str += ", MASTER_SSL=1"
        cmd_str += ", MASTER_SSL_CA='#{ssl_ca_path}'" if ssl_ca_path
        cmd_str += ", MASTER_SSL_CIPHER = '#{ssl_master_cipher}'" if ssl_master_cipher

        if ssl_client_cert_path && ssl_client_key_path
            cmd_str +=
              ", MASTER_SSL_CERT='#{ssl_client_cert_path}', " + 
              "MASTER_SSL_KEY='#{ssl_client_key_path}'"
        end
      end

      result = mysql_root_cmd cmd_str 

      msg = "Changing master to #{new_master}"
      msg += " using SSL" if use_ssl
      if option_hash[:auto_position]
        msg += " with GTID auto-positioning. #{result}"
      else
        msg += " with coordinates (#{option_hash[:log_file]}, #{option_hash[:log_pos]}). #{result}"
      end
      output msg

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
      else
        output mysql_root_cmd "STOP SLAVE"
        @repl_paused = true
      end
      # Display and return the replication progress
      gtid_mode? ? gtid_executed_from_pool_master_string(true) : repl_binlog_coordinates(true)
    end
    alias stop_replication pause_replication
    
    # Starts replication, or restarts replication after a pause
    def resume_replication
      raise "This DB object has no master" unless master
      # Display the replication progress
      gtid_mode? ? gtid_executed_from_pool_master_string(true) : repl_binlog_coordinates(true)
      output "Resuming replication from #{@master}."
      output mysql_root_cmd "START SLAVE"
      @repl_paused = false
    end
    alias start_replication resume_replication

    # Stops replication at the same place on many nodes. Uses GTIDs if enabled, otherwise
    # uses binlog coordinates. Only works on hierarchical replication topologies if using GTID.
    def pause_replication_with(*db_list)
      raise "DB#pause_replication_with requires at least one DB as parameter!" unless db_list.size > 0
      my_pool = pool(true)
      if my_pool.gtid_mode?
        raise 'DB#pause_replication_with requires all nodes to be in the same pool!' unless db_list.all? {|db| db.pool(true) == my_pool}
        raise 'DB#pause_replication_with cannot be used on a master!' if self == my_pool.master || db_list.include?(my_pool.master)
      else
        raise 'Without GTID, DB#pause_replication_with requires all nodes to have the same master!' unless db_list.all? {|db| db.master == master}
      end
      
      db_list.unshift self unless db_list.include? self

      db_list.concurrent_each &:pause_replication
      furthest_replica = db_list.inject{|furthest_db, this_db| this_db.ahead_of?(furthest_db) ? this_db : furthest_db}
      if my_pool.gtid_mode?
        gtid_set = furthest_replica.gtid_executed_from_pool_master(true)
        # If no replicas have executed transactions from current pool master, fall back to
        # using the full gtid_executed. This is risky if any replicas have errant transactions
        # (i.e. something ran binlogged statements directly on a replica) since START SLAVE UNTIL
        # will never complete in this situation.
        if gtid_set.nil?
          output 'WARNING: no replicas have executed transactions from current pool master; something may be amiss'
          gtid_set = furthest_replica.gtid_executed(true)
        end
      else
        binlog_coord = furthest_replica.repl_binlog_coordinates(true)
      end
      db_list.select {|db| furthest_replica.ahead_of? db}.concurrent_each do |db|
        db.resume_replication_until(binlog_coord, gtid_set)
      end
      
      if db_list.any? {|db| furthest_replica.ahead_of?(db) || db.ahead_of?(furthest_replica)}
        raise 'Unexpectedly unable to stop slaves in the same position; perhaps something restarted replication?'
      end
    end

    # Resumes replication up to the specified binlog_coord (array of [logfile string, position int])
    # or gtid set (string). You must supply binlog_coord OR gtid_set, but not both; set the other
    # one to nil. 
    # This method blocks until the specified coordinates/gtids have been reached, up to a max of the
    # specified timeout, after which point it raises an exception.
    def resume_replication_until(binlog_coord, gtid_set=nil, timeout_sec=3600)
      if binlog_coord.nil? && !gtid_set.nil?
        output "Resuming replication until after GTID set #{gtid_set}, waiting for up to #{timeout_sec} seconds."
        output mysql_root_cmd "START SLAVE UNTIL SQL_AFTER_GTIDS = '#{gtid_set}'"
        result = query_return_first_value("SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS(?, ?)", gtid_set, timeout_sec)
        if result == -1
          mysql_root_cmd "STOP SLAVE" # safer than leaving an UNTIL replication condition set indefinitely
          raise "#{self} did not reach GTID set #{gtid_set} within #{timeout_sec} seconds. Stopping replication."
        end
      elsif !binlog_coord.nil? && gtid_set.nil?
        output "Resuming replication until master coords (#{binlog_coord[0]}, #{binlog_coord[1]}), waiting for up to #{timeout_sec} seconds."
        output mysql_root_cmd "START SLAVE UNTIL MASTER_LOG_FILE = '#{binlog_coord[0]}', MASTER_LOG_POS = #{binlog_coord[1]}"
        result = query_return_first_value("SELECT MASTER_POS_WAIT(?, ?, ?)", *binlog_coord, timeout_sec)
        if result == -1
          mysql_root_cmd "STOP SLAVE" # safer than leaving an UNTIL replication condition set indefinitely
          raise "#{self} did not reach master coords (#{binlog_coord[0]}, #{binlog_coord[1]}) within #{timeout_sec} seconds. Stopping replication."
        end
      else
        raise "DB#resume_replication_until requires EXACTLY ONE of binlog_coord or gtid_set to be non-nil"
      end
      
      # START SLAVE UNTIL will leave the slave io thread running, so we explicitly stop it
      output mysql_root_cmd "STOP SLAVE IO_THREAD"
      @repl_paused = true
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
      disable_monitoring
      targets.each {|t| t.disable_monitoring}
      pause_replication if master && ! @repl_paused
      change_master_options = {
        user:     repl_user || replication_credentials[:user],
        password: repl_pass || replication_credentials[:pass],
      }
      if pool(true).gtid_mode?
        change_master_options[:auto_position] = true
        gtid_executed_from_pool_master_string(true) # display gtid executed value
      else
        change_master_options[:log_file], change_master_options[:log_pos] = binlog_coordinates
      end
      clone_to!(targets)
      targets.each do |t|
        t.enable_monitoring
        t.change_master_to(self, change_master_options)
        t.enable_read_only!
      end
      resume_replication if @master # should already have happened from the clone_to! restart anyway, but just to be explicit
      catch_up_to_master 21600
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

      change_master_options = {
        user:     replication_credentials[:user],
        password: replication_credentials[:pass],
      }
      if pool(true).gtid_mode?
        change_master_options[:auto_position] = true
        gtid_executed_from_pool_master_string(true) # display gtid executed value
      else
        change_master_options[:log_file], change_master_options[:log_pos] = repl_binlog_coordinates
      end

      clone_to!(targets)
      targets.each do |t| 
        t.enable_monitoring
        t.change_master_to(master, change_master_options)
        t.enable_read_only!
      end
      [ self, targets ].flatten.each(&:resume_replication) # should already have happened from the clone_to! restart anyway, but just to be explicit
      [ self, targets ].flatten.concurrent_each{|n| n.catch_up_to_master 21600 }
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
      raise "Cannot obtain binlog coordinates of this master because binary logging is not enabled" unless hash[:file]
      output "Own binlog coordinates are (#{hash[:file]}, #{hash[:position].to_i})." if display_info
      [hash[:file], hash[:position].to_i]
    end
    
    # Returns the number of seconds behind the master the replication execution is,
    # as reported by SHOW SLAVE STATUS.
    def seconds_behind_master
      raise "This instance is not a slave" unless master
      lag = slave_status[:seconds_behind_master]
      lag == 'NULL' ? nil : lag.to_i
    end
    
    # Call this method on a replica to block until it catches up with its master.
    # If this doesn't happen within timeout (seconds), raises an exception.
    #
    # If the pool is currently receiving writes, this method monitors slave lag
    # and will wait for self's SECONDS_BEHIND_MASTER to reach 0 and stay at
    # 0 after repeated polls (based on threshold and poll_frequency). If a large
    # amount of slave lag is seen, polling frequency is automatically adjusted.
    # In other words, with default settings: checks slave lag every 5+ sec, and
    # returns true if slave lag is zero 3 times in a row. Gives up if this does
    # not occur within a one-hour period.
    #
    # If the pool is NOT receiving writes, this method bases its behavior on
    # coords/gtids (depending which is in use) and guarantees that self is at
    # the same position as its master.
    def catch_up_to_master(timeout=21600, threshold=3, poll_frequency=5)
      raise "This instance is not a slave" unless master
      resume_replication if @repl_paused
      if pool(true).gtid_mode?
        master_gtid_executed = master.gtid_executed(true)
        master_taking_writes = Proc.new {|db| db.gtid_executed != master_gtid_executed}
      else
        master_coords = master.binlog_coordinates(true)
        master_taking_writes = Proc.new {|db| db.binlog_coordinates != master_coords}
      end
      
      times_at_zero = 0
      start = Time.now.to_i
      output "Waiting to catch up to master"
      while (Time.now.to_i - start) < timeout
        lag = seconds_behind_master
        if lag == 0
          if master_taking_writes.call(master)
            master_taking_writes = Proc.new {true} # no need for subsequent re-checking
            times_at_zero += 1
            if times_at_zero >= threshold
              output "Caught up to master."
              return true
            end
          elsif !master.ahead_of? self
            output "Caught up to master completely (no writes occurring)."
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
    
    # Return true if this node's replication progress is ahead of the provided
    # node, or false otherwise. The nodes must be in the same pool for coordinates
    # to be comparable. Does not work in hierarchical replication scenarios unless GTID
    # is in use!
    def ahead_of?(node)
      my_pool = pool(true)
      raise "Node #{node} is not in the same pool as #{self}" unless node.pool(true) == my_pool
      
      if my_pool.gtid_mode?
        # Ordinarily we only want to concern ourselves with transactions that came from the
        # current pool master. BUT if the target node hasn't executed any of those, then we need
        # to look at other things as a workaround. If self DOES have transactions from pool
        # master and the other node doesn't, we know self is ahead; otherwise, look at full gtid
        # sets (not just from pool master) and see if one node has transactions the other does not.
        node_gtid_exec = node.gtid_executed_from_pool_master(true)
        if node_gtid_exec.nil?
          return true unless gtid_executed_from_pool_master(true).nil?
          self_has_extra = has_extra_transactions_vs? node
          node_has_extra = node.has_extra_transactions_vs? self
          raise "Cannot determine which node is ahead; both have disjoint extra transactions" if self_has_extra && node_has_extra
          return self_has_extra
        else
          return ahead_of_gtid? node_gtid_exec
        end
      else
        # Checks if the master in the pool is self or another node in the pool
        node_coords = (my_pool.master == node ? node.binlog_coordinates : node.repl_binlog_coordinates)
        return ahead_of_coordinates?(node_coords)
      end
    end

    def ahead_of_coordinates?(binlog_coord)
      my_pool = pool(true)
      my_coords = (my_pool.master == self ? binlog_coordinates : repl_binlog_coordinates)

      # Same coordinates: we're not "ahead"
      if my_coords == binlog_coord
        false
      
      # Same logfile: simply compare position
      elsif my_coords[0] == binlog_coord[0]
        my_coords[1] > binlog_coord[1]
        
      # Different logfile
      else
        my_logfile_num = my_coords[0].match(/^[a-zA-Z.0]+(\d+)$/)[1].to_i
        binlog_coord_logfile_num = binlog_coord[0].match(/^[a-zA-Z.0]+(\d+)$/)[1].to_i
        my_logfile_num > binlog_coord_logfile_num
      end
    end
    
    # Returns true if self has executed at least one transaction past the supplied gtid_set
    # The arg should only contain one uuid, obtained from gtid_executed_from_pool_master.
    # (With a full gtid_executed containing multiple uuids, the notion of "ahead" could be
    # undefined, as there's no implied ordering of uuids)
    def ahead_of_gtid?(gtid_set)
      self_progress = gtid_executed_from_pool_master
      
      # Don't try comparing to a node that hasn't executed any transactions from
      # current pool master. The definition of "ahead" in this situation could be
      # undefined, e.g. if self_progress is also nil. Instead in this case, use
      # another method like DB#has_extra_transactions_vs? to get a sane result.
      raise "Cannot call DB#ahead_of_gtid? with a nil arg" if gtid_set.nil?
      
      if self_progress.nil?
        # self hasn't executed transactions from pool master but other node has: we know we're behind
        false
      elsif gtid_set == self_progress
        # same gtid_executed: we're not "ahead"
        false
      else
        result = query_return_first_value("SELECT gtid_subset(?, ?)", gtid_set, self_progress)
        # a 1 result means "gtid_set is a subset of self_progress" which means self has executed
        # all of these transactions already
        result == 1
      end
    end
    
    def binary_log_enabled?
      global_variables[:log_bin].downcase == 'on'
    end
    
    def gtid_mode?
      # to_s is needed because global_variables[:gtid_mode] is nil for mysql versions prior to 5.6
      global_variables[:gtid_mode].to_s.downcase == 'on'
    end

    # note: as implemented, this is Percona Server specific. MySQL 5.6 has no equivalent functionality.
    # https://www.percona.com/doc/percona-server/5.6/flexibility/online_gtid_deployment.html
    # WebScaleSQL ties this in to read_only instead of making it a separate variable. Percona presumably
    # used a separate variable to support master-master pools, but Jetpants does not support these.
    def gtid_deployment_step?
      # to_s is needed because global_variables[:gtid_deployment_step] is nil for non-Percona Server,
      # or for Percona Server versions prior to 5.6.22-72.0
      global_variables[:gtid_deployment_step].to_s.downcase == 'on'
    end
    
    # Returns true if the DB supports online GTID rollout (i.e. Percona Server >= 5.6.22-72.0), or
    # false otherwise.
    def supports_online_gtid_rollout?
      global_variables.has_key? :gtid_deployment_step
    end
    
    # Dynamically disables gtid_deployment_step. Intended for use by a GTID rollout script or shard merge.
    # Nothing else should manipulate this variable, and ordinarily this variable should always be disabled.
    # If ever accidentally left enabled on a DB that gets promoted to master, it will stop assigning
    # GTIDs to new transactions.
    def enable_gtid_deployment_step!
      raise "Cannot enable gtid_deployment_step without gtid_mode" unless gtid_mode?
      return if gtid_deployment_step?
      raise "#{self} does not support gtid_deployment_step" unless supports_online_gtid_rollout?
      toggle_gtid_deployment_step(true)
    end
    
    # Dynamically disables gtid_deployment_step. Intended for use by a GTID rollout script or shard merge.
    def disable_gtid_deployment_step!
      return unless gtid_deployment_step?
      raise "#{self} does not support gtid_deployment_step" unless supports_online_gtid_rollout?
      toggle_gtid_deployment_step(false)
    end
    
    # Helper for enable_gtid_deployment_step! / disable_gtid_deployment_step!
    # Arg should only be a boolean, not 0/1 or "OFF"/"ON"
    def toggle_gtid_deployment_step(enable)
      value = (enable ? 1 : 0)
      mysql_root_cmd "SET GLOBAL gtid_deployment_step = #{value}"
    end

    # This intentionally executes a query instead of using SHOW GLOBAL VARIABLES, because the value
    # can get quite long, and SHOW GLOBAL VARIABLES truncates its output
    def gtid_executed(display_info=false)
      result = query_return_first_value "SELECT @@global.gtid_executed"
      result.gsub! "\n", ''
      output "gtid_executed is #{result}" if display_info
      result
    end
    
    # Returns the portion of self's gtid_executed relevant to just the pool's current master.
    # This is useful for comparing replication progress without potentially getting tripped-up
    # by missing transactions from other server_uuids. (which shouldn't normally happen anyway,
    # but if they do, it would cause problems with WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS if using
    # the full gtid_executed set)
    # If the DB has not executed any transactions from the pool master yet, returns nil! This
    # is intentional, so that callers can handle this situation as appropriate.
    def gtid_executed_from_pool_master(display_info=false)
      uuid = pool(true).master_uuid
      gtid_sets = gtid_executed.split(',')
      # This intentionally will be nil if no transactions executed from pool master
      result = gtid_sets.select {|gs| gs.start_with? "#{uuid}:"}.first
      if display_info
        if result.nil?
          output "gtid_executed does not contain any transactions from pool master #{uuid}!"
        else
          output "gtid_executed from pool master is #{result}"
        end
      end
      result
    end
    
    # Like gtid_executed_from_pool_master, but instead of nil in the no-transactions case,
    # returns a user-friendly uuid:none string. In the cannot-determine-master-UUID case,
    # does not throw an exception.
    # This method should generally only be used for display purposes, not for any MySQL
    # function calls!
    def gtid_executed_from_pool_master_string(display_info=false)
      result = gtid_executed_from_pool_master(display_info) rescue "unknown:unknown"
      if result.nil?
        uuid = pool(true).master_uuid
        "#{uuid}:none"
      else
        result
      end
    end
    
    def gtid_purged
      query_return_first_value "SELECT @@global.gtid_purged"
    end

    # After cloning a DB that uses GTID, it is necessary to set the new replica's
    # gtid_purged to be equal to the source's gtid_executed, since we don't copy
    # binlogs in the cloning process. This is the only situation in which this
    # method should (or even can) be used.
    def gtid_purged=(gtid_set)
      raise "DB#gtid_purged= requires gtid_mode" unless gtid_mode?
      raise "DB#gtid_purged= cannot be called on a node with replicas" if has_slaves?
      raise "gtid_purged may only be set if gtid_executed is empty" unless gtid_executed == ''
      mysql_root_cmd "SET GLOBAL gtid_purged = '#{gtid_set}'"
      
      # To avoid potential problems with binlog_gtid_simple_recovery, immediately roll to
      # a new binlog file and then flush all previous binlog files. This ensures the oldest
      # binlog file contains the correct info to set gtid_purged upon restart.
      mysql_root_cmd "FLUSH LOCAL BINARY LOGS"
      log_file = binlog_coordinates(false)[0]
      mysql_root_cmd "PURGE BINARY LOGS TO '#{log_file}'"
    end

    def server_uuid
      query_return_first_value "SELECT @@global.server_uuid"
    end

    # Returns true if self has executed transactions that relative_to_node has not.
    # relative_to_node must be in the same pool as self.
    # NOTE: if there are writes occurring in the pool, this method should only be used
    # with a relative_to_node that is higher on the replication chain than self (i.e.
    # relative_to_node is self's master, or self's master's master).
    def has_extra_transactions_vs?(relative_to_node)
      my_pool = pool(true)
      raise "Node #{relative_to_node} is not in the same pool as #{self}" unless relative_to_node.pool(true) == my_pool
      raise "DB#has_extra_transactions_vs? requires gtid_mode" unless my_pool.gtid_mode?
      
      # We specifically obtain gtid_executed for self BEFORE obtaining it for the
      # other node. That way, if writes are still occurring and the other node is
      # higher on the replication chain, we won't get thrown off by the new writes
      # looking like transactions that ran on the replica but not the master.
      self_gtid_exec = self.gtid_executed
      other_gtid_exec = relative_to_node.gtid_executed
      result = query_return_first_value("SELECT gtid_subset(?, ?)", self_gtid_exec, other_gtid_exec)
      # a 0 result means "not a subset" which indicates self_gtid_exec contains transactions missing
      # from other_gtid_exec
      result == 0
    end
    
    # Returns true if self has already purged binlogs containing transactions
    # that the target node would need. This means that if we promoted self to
    # be the master of the target node, replication would break on the target
    # node.
    def purged_transactions_needed_by?(node)
      my_pool = pool(true)
      raise "Node #{node} is not in the same pool as #{self}" unless node.pool(true) == my_pool
      raise "DB#purged_transactions_needed_by? requires gtid_mode" unless my_pool.gtid_mode?
      
      result = node.query_return_first_value("SELECT gtid_subset(?, @@global.gtid_executed)", gtid_purged)
      # a 0 result means "not a subset" which indicates there are transactions on self's
      # gtid_purged list that have not been executed on node
      result == 0
    end
    
    # When a master dies and the new-master candidate is potentially not the
    # furthest-ahead replica, call this method on the new-master to have it
    # catch up on missing transactions from whichever sibling is further ahead.
    # Returns true if the transactions were applied successfully or if no
    # catch up was necessary, or false if not successful.
    # siblings should be an array of other DBs in the pool at the same level as
    # self (but excluding self). timeout and progress_interval are in seconds.
    def replay_missing_transactions(siblings, change_master_options, timeout=300, progress_interval=5)
      if siblings.empty?
        output "Cannot replay missing transactions -- no siblings provided"
        return false
      end
      if replicating? || pool(true).master.running?
        output "DB#replay_missing_transactions may only be used in a dead-master scenario"
        return false
      end
      furthest_replica = siblings.inject{|furthest_db, this_db| this_db.ahead_of?(furthest_db) ? this_db : furthest_db}
      unless furthest_replica.ahead_of? self
        output "Selected new master is already up-to-date with all transactions executed by other replicas"
        return true
      end

      if furthest_replica.purged_transactions_needed_by? self
        output "WARNING: Needs missing transactions from furthest-ahead replica #{furthest_replica}, but they have already been purged!"
        output "This means we cannot replay these transactions on the new master, so they will effectively be lost."
        gtid_executed(true)
        furthest_replica.gtid_executed(true)
        return false
      end

      output "Obtaining missing transactions by temporarily replicating from furthest-ahead node #{furthest_replica}"
      gtid_executed(true)
      furthest_replica.gtid_executed(true)
      change_master_to furthest_replica, change_master_options
      resume_replication
      attempts = timeout / progress_interval
      while attempts > 0 && replicating? && furthest_replica.has_extra_transactions_vs?(self) do
        sleep progress_interval
        attempts -= 1
        gtid_executed(true)
      end
      disable_replication!
      if furthest_replica.has_extra_transactions_vs?(self)
        output "WARNING: Unable to complete replaying missing transactions."
        output "Giving up and proceeding with the rest of promotion. Some transactions will effecitvely be lost."
        false
      else
        output "Successfully replayed missing transactions from furthest-ahead node #{furthest_replica}"
        true
      end
    end

    # Modifies the replication topology; supports two different cases:
    # Case 1: A tiered slave is re-pointed one level up, i.e. to be sibling of its current master.
    # Case 2: A sibling slave is re-pointed one level down, i.e. to be replicating from one of its sibling.
    def repoint_to(new_master_node)
      raise "DB#repoint_to can only be called on a slave" unless is_slave?
      # Case 1, we compare the master two levels up with the master_node provided as argument, if equals we can change the topology by pausing replication of slave's master and retrieving replication coordinates to set replication from new_master_node.
      if master.master == new_master_node
        orig_master_node = master
        orig_master_node.pause_replication
        unless seconds_behind_master == 0
          catch_up_to_master
        end
        log, pos = orig_master_node.repl_binlog_coordinates
        raise "No replication status found." if log.nil?
        # If log.nil? returns true, it means replication is not set up on the orig_master_node hence pause_replication will fail and resume_replication will fail too.
        stop_replication
        orig_master_node.resume_replication
      # Case 2, we compare master of both slave and new_master_node, if it equals then we determine that both slave and new_master_node are siblings and we can retrieve binlog coordinates of new_master_node to set up slave replicating from it
      elsif master == new_master_node.master
        pause_replication_with new_master_node
        log, pos = new_master_node.binlog_coordinates
        new_master_node.resume_replication
        raise "Binary logging not enabled." if log.nil?
        stop_replication
        # If log.nil? returns true in this case, it means binary logging must not be enabled here. We cannot setup replication without it.
      else
        raise "DB#repoint_to can work only with cases where Node-to-repoint is a sibling with its future master OR where Node-to-repoint is a tiered slave one level down the future master"
      end
      change_master_options = {
        user: new_master_node.replication_credentials[:user],
        password: new_master_node.replication_credentials[:pass],
      }
      if pool(true).global_variables[:gtid_mode].to_s.downcase == "on"
        change_master_options[:auto_position] = 1
      else
        change_master_options[:log_file], change_master_options[:log_pos] = log, pos
      end
      # CHANGE MASTER TO .. command
      reset_replication!
      change_master_to new_master_node, change_master_options

      change_master_options[:master_host] = new_master_node.ip
      change_master_options[:master_user] = change_master_options.delete :user
      change_master_options[:master_log_file] = change_master_options.delete :log_file
      change_master_options[:exec_master_log_pos] = change_master_options.delete :log_pos
      change_master_options[:exec_master_log_pos] = change_master_options[:exec_master_log_pos].to_s
      change_master_options.delete :password
      change_master_options.each do |option, value|
        raise "Unexpected slave status value for #{option} in replica #{self} after promotion" unless slave_status[option] == value
      end
      resume_replication unless replicating?
      catch_up_to_master
    end

  end
end
