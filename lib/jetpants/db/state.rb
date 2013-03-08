module Jetpants
  
  #--
  # State accessors ############################################################
  #++
  
  class DB
    # Returns the Jetpants::DB instance that is the master of this instance, or false if
    # there isn't one, or nil if we can't tell because this instance isn't running.
    def master
      return nil unless running? || @master
      probe if @master.nil?
      @master
    end
    
    # Returns an Array of Jetpants::DB instances that are slaving from this instance,
    # or nil if we can't tell because this instance isn't running.
    def slaves
      return nil unless running? || @slaves
      probe if @slaves.nil?
      @slaves
    end
    
    # Returns true if replication is paused on this instance, false if it isn't, or
    # nil if this instance isn't a slave (or if we can't tell because the instance
    # isn't running)
    def repl_paused?
      return nil unless master
      probe if @repl_paused.nil?
      @repl_paused
    end

    # Returns true if MySQL is running for this instance, false otherwise.
    # Note that if the host isn't available/online/reachable, we consider
    # MySQL to not be running.
    def running?
      probe if @running.nil?
      @running
    end
    
    # Returns true if we've probed this MySQL instance already.  Several
    # methods trigger a probe, including master, slaves, repl_paused?, and
    # running?.
    def probed?
      [@master, @slaves, @running].compact.count >= 3
    end

    # Probes this instance to discover its status, master, and slaves. Several
    # other methods trigger a probe automatically, including master, slaves,
    # repl_paused?, and running?.
    # Ordinarily this method won't re-probe an instance that has already been
    # probed, unless you pass force=true.  This can be useful if something
    # external to Jetpants has changed a DB's state while Jetpants is running.
    # For example, if you're using jetpants console and, for whatever reason,
    # you stop replication on a slave manually outside of Jetpants.  In this
    # case you will need to force a probe so that Jetpants learns about the
    # change.
    def probe(force=false)
      return if probed? && !force
      output "Probing MySQL installation"
      probe_running
      probe_master
      probe_slaves
      self
    end
    
    # Alias for probe(true)
    def probe!() probe(true) end
    
    # Returns true if the MySQL slave I/O thread and slave SQL thread are
    # both running, false otherwise.  Note that this always checks the current
    # actual state of the instance, as opposed to DB#repl_paused? which just
    # remembers the state from the previous probe and any actions since then.
    def replicating?
      status = slave_status
      [status[:slave_io_running], status[:slave_sql_running]].all? {|s| s && s.downcase == 'yes'}
    end

    # Returns true if this instance has a master, false otherwise.
    def is_slave?
      !!master
    end

    # Returns true if this instance had at least one slave when it was last
    # probed, false otherwise. (This method will indirectly force a probe if
    # the instance hasn't been probed before.)
    def has_slaves?
      slaves.count > 0
    end

    # Returns true if the global READ_ONLY variable is set, false otherwise.
    def read_only?
      global_variables[:read_only].downcase == 'on'
    end

    # Confirms instance has no more than [max] connections currently
    # (AS VISIBLE TO THE APP USER), and in [interval] seconds hasn't
    # received more than [threshold] additional connections.
    # You may need to adjust max if running multiple query killers,
    # monitoring agents, etc.
    def taking_connections?(max=4, interval=2.0, threshold=1)
      current_conns = query_return_array('show processlist').count
      return true if current_conns > max
      conn_counter = global_status[:Connections].to_i
      sleep(interval)
      global_status[:Connections].to_i - conn_counter > threshold
    end

    # Gets the max theads connected over a time period
    def max_threads_running(tries=8, interval=1.0)
      poll_status_value(:Threads_running,:max, tries, interval)
    end

    # Gets the max or avg for a mysql value
    def poll_status_value(field, type=:max, tries=8, interval=1.0)
      max = 0
      sum = 0
      tries.times do
        value = global_status[field].to_i
        max = value unless max > value
        sum += value
        sleep(interval)
      end
      if type == :max
        max
      elsif type == :avg
        sum.to_f/tries.to_f
      end
    end
    
    # Confirms the binlog of this node has not moved during a duration
    # of [interval] seconds.
    def taking_writes?(interval=5.0)
      coords = binlog_coordinates
      sleep(interval)
      coords != binlog_coordinates
    end
    
    # Returns true if this instance appears to be a standby slave,
    # false otherwise. Note that "standby" in this case is based
    # on whether the slave is actively receiving connections, not
    # based on any Pool's understanding of the slave's state. An asset-
    # tracker plugin may want to override this to determine standby
    # status differently.
    def is_standby?
      !(running?) || (is_slave? && !taking_connections?)
    end
    
    # Jetpants supports a notion of dedicated backup machines, containing one
    # or more MySQL instances that are considered "backup slaves", which will
    # never be promoted to serve production queries.  The default
    # implementation identifies these by a hostname beginning with "backup".
    # You may want to override this with a plugin to use a different scheme
    # if your architecture contains a similar type of node.
    def for_backups?
      @host.hostname.start_with? 'backup'
    end
    
    # Returns true if the node can be promoted to be the master of its pool,
    # false otherwise (also false if node is ALREADY the master)
    # Don't use this in hierarchical replication scenarios, result may be
    # unexpected.
    def promotable_to_master?(detect_version_mismatches=true)
      # backup_slaves are non-promotable
      return false if for_backups?
      
      # already the master
      p = pool(true)
      return false if p.master == self
      
      # ordinarily, cannot promote a slave that's running a higher version of
      # MySQL than any other node in the pool.
      if detect_version_mismatches
        p.nodes.all? {|db| db == self || !db.available? || db.version_cmp(self) >= 0}
      else
        true
      end
    end
    
    # Returns a hash mapping global MySQL variables (as symbols)
    # to their values (as strings).
    def global_variables
      query_return_array('show global variables').reduce do |variables, variable|
        variables[variable[:Variable_name].to_sym] = variable[:Value]
        variables
      end
    end
    
    # Returns a hash mapping global MySQL status fields (as symbols)
    # to their values (as strings).
    def global_status
      query_return_array('show global status').reduce do |variables, variable|
        variables[variable[:Variable_name].to_sym] = variable[:Value]
        variables      
      end
    end
    
    # Returns an array of integers representing the version of the MySQL server.
    # For example, Percona Server 5.5.27-rel28.1-log would return [5, 5, 27]
    def version_tuple
      result = nil
      if running?
        # If the server is running, we can just query it
        result = global_variables[:version].split('.', 3).map(&:to_i) rescue nil
      end
      if result.nil?
        # Otherwise we need to parse the output of mysqld --version
        output = ssh_cmd 'mysqld --version'
        matches = output.downcase.match('ver\s*(\d+)\.(\d+)\.(\d+)')
        raise "Unable to determine version for #{self}" unless matches
        result = matches[1, 3].map(&:to_i)
      end
      result
    end
    
    # Return a string representing the version. The precision indicates how
    # many major/minor version numbers to return.
    # ie, on 5.5.29, normalized_version(3) returns '5.5.29',
    # normalized_version(2) returns '5.5', and normalized_version(1) returns '5'
    def normalized_version(precision=2)
      raise "Invalid precision #{precision}" if precision < 1 || precision > 3
      version_tuple[0, precision].join('.')
    end
    
    # Returns -1 if self is running a lower version than db; 1 if self is running
    # a higher version; and 0 if running same version.
    def version_cmp(db, precision=2)
      raise "Invalid precision #{precision}" if precision < 1 || precision > 3
      my_tuple = version_tuple[0, precision]
      other_tuple = db.version_tuple[0, precision]
      my_tuple.each_with_index do |subver, i|
        return -1 if subver < other_tuple[i]
        return 1 if subver > other_tuple[i]
      end
      0
    end
    
    # Returns the Jetpants::Pool that this instance belongs to, if any.
    # Can optionally create an anonymous pool if no pool was found. This anonymous
    # pool intentionally has a blank sync_configuration implementation.
    def pool(create_if_missing=false)
      result = Jetpants.topology.pool(self)
      if !result && master
        result ||= Jetpants.topology.pool(master)
      end
      if !result && create_if_missing
        pool_master = master || self
        result = Pool.new('anon_pool_' + pool_master.ip.tr('.', ''), pool_master)
        def result.sync_configuration; end
      end
      return result
    end
    
    # Determines the DB's role in its pool. Returns either :master,
    # :active_slave, :standby_slave, or :backup_slave.
    #
    # Note that we consider a node with no master and no slaves to be
    # a :master, since we can't determine if it had slaves but they're
    # just offline/dead, vs it being an orphaned machine.
    #
    # In hierarchical replication scenarios (such as the child shard
    # masters in the middle of a shard split), we return :master if
    # Jetpants.topology considers the node to be the master for a pool.
    def role
      p = pool
      case
      when !@master then :master                                # nodes that aren't slaves (including orphans) 
      when p.master == self then :master                        # nodes that the topology thinks are masters
      when for_backups? then :backup_slave
      when p && p.active_slave_weights[self] then :active_slave # if pool in topology, determine based on expected/ideal state
      when !p && !is_standby? then :active_slave                # if pool missing from topology, determine based on actual state
      else :standby_slave
      end
    end
    
    # Returns the data set size in bytes (if in_gb is false or omitted) or in gigabytes
    # (if in_gb is true).  Note that this is actually in gibibytes (2^30) rather than
    # a metric gigabyte.  This puts it on the same scale as the output to tools like 
    # "du -h" and "df -h".
    def data_set_size(in_gb=false)
      bytes = dir_size("#{mysql_directory}/#{app_schema}") + dir_size("#{mysql_directory}/ibdata1")
      in_gb ? (bytes / 1073741824.0).round : bytes
    end
    
    def mount_stats(mount=false)
      mount ||= mysql_directory

      host.mount_stats(mount)
    end
    
    ###### Private methods #####################################################
    
    private
    
    # Check if mysqld is running.
    # If your Linux distro's implementation of "service" returns output formatted
    # differently than what we check for here (which matches RHEL and Ubuntu),
    # you will need to override this method in a plugin! Or if you submit a patch
    # we'd be happy to merge it upstream to support more distros.
    def probe_running
      if @host.available?
        status = service(:status, 'mysql').downcase
        # mysql is running if the output of "service mysql status" doesn't include any of these strings
        not_running_strings = ['not running', 'stop/waiting']
        @running = not_running_strings.none? {|str| status.include? str}
      else
        @running = false
      end
    end
    
    # Checks slave status to determine master and whether replication is paused
    # An asset-tracker plugin may want to implement DB#after_probe_master to
    # populate @master even if @running is false.
    def probe_master
      return unless @running # leaves @master as nil to indicate unknown state
      status = slave_status
      if !status || status.count < 1
        @master = false
      else
        @master = self.class.new(status[:master_host], status[:master_port])
        if status[:slave_io_running] != status[:slave_sql_running]
          output "One replication thread is stopped and the other is not."
          if Jetpants.verify_replication
            output "You must repair this node manually, OR remove it from its pool permanently if it is unrecoverable."
            raise "Fatal replication problem on #{self}"
          end
          pause_replication
        else
          @repl_paused = (status[:slave_io_running].downcase == 'no')
        end
      end
    end
    
    # Check processlist as root to determine replication clients.  This assumes
    # you're running only one MySQL instance per machine, and all MySQL instances
    # use the standard port 3306.  This is a limitation of SHOW PROCESSLIST not
    # containing the slave's listening port.
    #
    # An asset-tracker plugin may want to implement DB#after_probe_slaves to
    # populate @slaves even if @running is false.
    #
    # Plugins may want to override DB#probe_slaves itself too, if running multiple
    # MySQL instances per physical machine. In this case you'll want to use 
    # SHOW SLAVE HOSTS, and all slaves must be using the --report-host option if
    # using MySQL < 5.5.3.
    def probe_slaves
      return unless @running # leaves @slaves as nil to indicate unknown state
      @slaves = []
      slaves_mutex = Mutex.new
      processes = mysql_root_cmd("SHOW PROCESSLIST", :terminator => ';').split("\n")
      
      # We have to de-dupe the output, since it's possible in weird edge cases for
      # the same slave to be listed twice
      ips = {}
      processes.grep(/Binlog Dump/).each do |p|
        tokens = p.split
        ip, dummy = tokens[2].split ':'
        ips[ip] = true
      end
      
      ips.keys.concurrent_each do |ip|
        db = ip.to_db
        db.probe
        slaves_mutex.synchronize {@slaves << db if db.master == self}
      end
    end
    
  end
end
