require 'json'
require 'db'

module Jetpants
  
  # a Pool represents a group of database instances (Jetpants::DB objects).
  #
  # The default implementation assumes that a Pool contains:
  # * 1 master
  # * 0 or more slaves, falling into one of these categories:
  #   * active slaves (actively taking production read queries)
  #   * standby slaves (for HA, promotable if a master or active slave fails + used to clone new replacements)
  #   * backup slaves (dedicated for backups and background jobs, never put into prod, potentially different hardware spec)
  #
  # Plugins may of course override this extensively, to support different
  # topologies, such as master-master trees.
  #
  # Many of these methods are only useful in conjunction with an asset-tracker /
  # configuration-generator plugin
  class Pool
    include CallbackHandler
    include Output

    # human-readable String name of pool
    attr_reader   :name
    
    # Jetpants::DB object that is the pool's master
    attr_reader   :master

    # Array of strings containing other equivalent names for this pool
    attr_reader   :aliases
    
    # Can be used to store a name that refers to just the active_slaves, for
    # instance if your framework isn't smart enough to know about master/slave
    # relationships.  Safe to leave as nil otherwise. Has no effect in Jetpants,
    # but an asset tracker / config generator plugin may include this in the
    # generated config file.
    attr_accessor :slave_name
    
    # Hash mapping DB object => weight, for active (read) slaves. Default weight
    # is 100. Safe to leave at default if your app framework doesn't support
    # different weights for individual read slaves. Weights have no effect inside
    # Jetpants, but any asset tracker / config generator plugin can carry them
    # through to the config file.
    attr_reader   :active_slave_weights
    
    # If the master also receives read queries, this stores its weight. Set to 0
    # if the master does not receive read queries (which is the default). This
    # has no effect inside of Jetpants, but can be used by an asset tracker /
    # config generator plugin to carry the value through to the config file.
    attr_accessor :master_read_weight

    # this is a list of nodes which have been claimed as spares, but which
    # won't show up in the slaves list
    attr_accessor :claimed_nodes
    
    def initialize(name, master)
      @name = name
      @slave_name = false
      @aliases = []
      @master = master.to_db || nil
      @master_read_weight = 0
      @active_slave_weights = {}
      @tables = nil
      @probe_lock = Mutex.new
      @claimed_nodes = []
      @gtid_mode = nil
      @master_uuid = nil
    end

    def change_master_to! new_master
      @master = new_master
      @master_uuid = nil # clear any memoized value
    end

    # Returns all slaves, or pass in :active, :standby, or :backup to receive slaves
    # just of a particular type
    def slaves(type=false)
      case type
      when :active_slave,  :active  then active_slaves
      when :standby_slave, :standby then standby_slaves
      when :backup_slave,  :backup  then backup_slaves
      when false                    then @master.slaves
      else []
      end
    end
    alias :running_slaves :slaves
    
    # Returns an array of Jetpants::DB objects.
    # Active slaves are ones that receive read queries from your application.
    def active_slaves
      @master.slaves.select {|sl| @active_slave_weights[sl]}
    end
    
    # Returns an array of Jetpants::DB objects.
    # Standby slaves do not receive queries from your application. These are for high availability.
    # They can be turned into active slaves or even the master, and can also be used for cloning
    # additional slaves.
    def standby_slaves
      @master.slaves.reject {|sl| @active_slave_weights[sl] || sl.for_backups?}
    end
    
    # Returns an array of Jetpants::DB objects.
    # Backup slaves are never promoted to active or master. They are for dedicated backup purposes.
    # They may be a different/cheaper hardware spec than other slaves.
    def backup_slaves
      @master.slaves.reject {|sl| @active_slave_weights[sl] || !sl.for_backups?}
    end
    
    # returns a flat array of all Jetpants::DB objects in the pool: the master and
    # all slaves of all types.
    def nodes
      [master, slaves].flatten.compact
    end

    # Look at a database in the pool (preferably a standby slave, but will check
    # active slave or master if nothing else is available) and retrieve a list of
    # tables, detecting their schema
    def probe_tables
      master.probe

      @probe_lock.synchronize do
        return unless @tables.nil?

        db = standby_slaves.last || active_slaves.last || master
        if db && db.running?
          output "Probing tables via #{db}"
        else
          output "Warning: unable to probe tables"
          return
        end
      
        @tables = []
        sql = "SHOW TABLES"
        db.query_return_array(sql).each do |tbl|
          table_name = tbl.values.first
          @tables << db.detect_table_schema(table_name)
        end
      end
    end
    
    # Returns a list of table objects for this pool
    def tables
      self.probe_tables unless @tables
      @tables
    end

    # Queries whether a pool has a table with a given name
    # note that this is the string name of the table and not an object
    def has_table?(table)
      tables.map(&:to_s).include?(table)
    end

    # Retrieve the table object for a given table name
    def get_table(table)
      raise "Pool #{self} does not have table #{table}" unless has_table? table

      @tables.select{|tb| tb.to_s == table}.first
    end

    # Informs Jetpants that slave_db is an active slave. Potentially used by 
    # plugins, such as in Topology at start-up time.
    def has_active_slave(slave_db, weight=100)
      slave_db = slave_db.to_db
      raise "Attempt to mark a DB as its own active slave" if slave_db == @master
      @active_slave_weights[slave_db] = weight
    end
    
    # Turns a standby slave into an active slave, giving it the specified read weight.
    # Syncs the pool's configuration afterwards. It's up to your asset tracker plugin to
    # actually do something with this information.
    def mark_slave_active(slave_db, weight=100)
      raise "Attempt to make a backup slave be an active slave" if slave_db.for_backups?
      has_active_slave slave_db, weight
      sync_configuration
    end
    
    # Turns an active slave into a standby slave. Syncs the pool's configuration afterwards.
    # It's up to your asset tracker plugin to actually do something with this information.
    def mark_slave_standby(slave_db)
      slave_db = slave_db.to_db
      raise "Cannot call mark_slave_standby on a master" if slave_db == @master
      @active_slave_weights.delete(slave_db)
      sync_configuration
    end
    
    # Remove a slave from a pool entirely. This is destructive, ie, it does a
    # RESET SLAVE on the db.
    #
    # Note that a plugin may want to override this (or implement after_remove_slave!)
    # to actually sync the change to an asset tracker, depending on how the plugin
    # implements Pool#sync_configuration. (If the implementation makes sync_configuration
    # work by iterating over the pool's current slaves to update their status/role/pool, it 
    # won't see any slaves that have been removed, and therefore won't update them.)
    #
    # This method has no effect on slaves that are unavailable via SSH or have MySQL
    # stopped, because these are only considered to be in the pool if your asset tracker
    # plugin intentionally adds them. Such plugins could also handle this in the
    # after_remove_slave! callback.
    def remove_slave!(slave_db)
      raise "Slave is not in this pool" unless slave_db.pool == self
      return false unless (slave_db.running? && slave_db.available?)
      slave_db.disable_monitoring
      slave_db.disable_replication!
      sync_configuration # may or may not be sufficient -- see note above.
    end
    
    # Informs this pool that it has an alias. A pool may have any number of aliases.
    def add_alias(name)
      if @aliases.include? name
        false
      else
        @aliases << name
        true
      end
    end

    # This function aids in providing the information about master/slaves discovered.
    def summary_info(node, counter, tab, extended_info=false)
      if extended_info
        details = {}
        if !node.running?
          details[node] = {coordinates: ['unknown'], lag: 'N/A', gtid_exec: 'unknown'}
        elsif node == @master and !node.is_slave?
          details[node] = {lag: 'N/A'}
          if gtid_mode?
            details[node][:gtid_exec] = node.gtid_executed_from_pool_master_string
          else
            details[node][:coordinates] = node.binlog_coordinates(false)
          end
        else
          lag = node.seconds_behind_master
          lag_str = lag.nil? ? 'NULL' : lag.to_s + 's'
          details[node] = {lag: lag_str}
          if gtid_mode?
            details[node][:gtid_exec] = node.gtid_executed_from_pool_master_string
          else
            details[node][:coordinates] = node.repl_binlog_coordinates(false)
          end
        end
      end

      # tabs below takes care of the indentation depending on the level of replication chain.
      tabs = '    ' *  (tab + 1)
      
      # Prepare the extended_info if needed
      binlog_pos = ''
      slave_lag = ''
      if extended_info
        slave_lag = "lag=#{details[node][:lag]}" unless node == @master && !node.is_slave?
        binlog_pos = gtid_mode? ? details[node][:gtid_exec] : details[node][:coordinates].join(':')
      end
      
      if node == @master and !node.is_slave?
        # Preparing the data_set_size and pool alias text
        alias_text = @aliases.count > 0 ? '  (aliases: ' + @aliases.join(', ') + ')' : ''
        data_size = @master.running? ? "[#{master.data_set_size(true)}GB]" : ''
        state_text = (respond_to?(:state) && state != :ready ? "  (state: #{state})" : '')
        print "#{name}#{alias_text}#{state_text}  #{data_size}\n"
        print "\tmaster          = %-15s %-32s %s\n" % [node.ip, node.hostname, binlog_pos]

      else
        # Determine the slave type below
        type = node.role.to_s.split('_').first
        format_str = "%s%-7s slave #{counter + 1} = %-15s %-32s " + (gtid_mode? ? "%-46s" : "%-26s") + " %s\n"
        print format_str % [tabs, type, node.ip, node.hostname, binlog_pos, slave_lag]
      end
    end

    # Displays a summary of the pool's members. This outputs immediately instead
    # of returning a string, so that you can invoke something like:
    #    Jetpants.topology.pools.each &:summary
    # to easily display a summary.
    def summary(extended_info=false, with_children=false, node=@master, depth=1)
      probe

      i = 0
      summary_info(node, i, depth, extended_info)
      slave_list = node.slaves
      slave_roles = Hash.new
      slave_list.each { |slave| slave_roles[slave] = slave.role }
      Hash[slave_roles.sort_by{ |k, v| v }].keys.each_with_index do |s, i|
        summary_info(s, i, depth, extended_info)
        if s.has_slaves?
          s.slaves.sort.each do |slave|
            summary(extended_info, with_children, slave, depth + 1)
          end
        end
      end
      true
    end

    # Returns an array of DBs in this pool that are candidates for promotion to new master.
    # NOTE: doesn't yet handle hierarchical replication scenarios. This method currently
    # only considers direct slaves of the pool master, by virtue of how Pool#nodes works.
    #
    # The enslaving_old_master arg determines whether or not the current pool master would
    # become a replica (enslaving_old_master=true) vs being removed from the pool
    # (enslaving_old_master=false). This just determines whether it is included in
    # version comparison logic, purged binlog logic, etc. The *result* of this method will
    # always exclude the current master regardless, as it doesn't make sense to consider
    # a node that's already the master to be "promotable".
    def promotable_nodes(enslaving_old_master=true)
      # Array of pool members, either including or excluding the old master as requested
      comparisons = nodes.select &:running?
      comparisons.delete master unless enslaving_old_master
      
      # Keep track of up to one "last resort" DB, which will only be promoted if
      # there are no other candidates. The score int allows ranking of last resorts,
      # to figure out the "least bad" promotion candidate in an emergency.
      last_resort_candidate = nil
      last_resort_score = 0
      last_resort_warning = ""
      consider_last_resort = Proc.new do |db, score, warning|
        if last_resort_candidate.nil? || last_resort_score < score
          last_resort_candidate = db
          last_resort_score = score
          last_resort_warning = warning
        end
      end
      
      # Build list of good candidates for promotion
      candidates = nodes.reject {|db| db == master || db.for_backups? || !db.running?}
      candidates.select! do |candidate|
        others = comparisons.reject {|db| db == candidate}
        
        # Node isn't promotable if it's running a higher version of MySQL than any of its future replicas
        next if others.any? {|db| candidate.version_cmp(db) > 0}
        
        if gtid_mode?
          # Ordinarily if gtid_mode is already in use in the pool, gtid_deployment_step
          # should not be enabled anywhere; this likely indicates either an incomplete
          # GTID rollout occurred, or an automation bug elsewhere. Reject the candidate
          # outright, unless the old master is dead, in which case we consider it as a
          # last resort with low score.
          if candidate.gtid_deployment_step?
            unless master.running?
              warning = "gtid_deployment_step is still enabled (indicating incomplete GTID rollout?), but there's no better candidate"
              consider_last_resort.call(candidate, 0, warning)
            end
            next
          end
          
          # See if any replicas would break if this candidate becomes master. If any will
          # break, only allow promotion as a last resort, with the score based on what
          # percentage will break
          breaking_count = others.count {|db| candidate.purged_transactions_needed_by? db}
          if breaking_count > 0
            breaking_pct = (100.0 * (breaking_count.to_f / others.length.to_f)).to_int
            score = 100 - breaking_pct
            warning = "#{breaking_pct}% of replicas will break upon promoting this node, but there's no better candidate"
            consider_last_resort.call(candidate, score, warning)
            next
          end
        end # gtid_mode checks
        
        # Only consider active slaves to be full candidates if the old master
        # is dead and we don't have GTID. In this situation, an active slave may
        # have the furthest replication progress. But in any other situation,
        # consider active slaves to be last resort, since promoting one would
        # also require converting a standby to be an active slave.
        if candidate.role == :active_slave && (gtid_mode? || master.running?)
          consider_last_resort.call(candidate, 100, "only promotion candidate is an active slave, since no standby slaves are suitable")
          next
        end
        
        # If we didn't hit a "next" statement in any of the above checks, the node is promotable
        true
      end
      
      if candidates.length == 0 && !last_resort_candidate.nil?
        last_resort_candidate.output "WARNING: #{last_resort_warning}"
        candidates << last_resort_candidate
      end
      candidates
    end

    # Demotes the pool's existing master, promoting a slave in its place.
    # The old master will become a slave of the new master if enslave_old_master is true,
    # unless the old master is unavailable/crashed.
    def master_promotion!(promoted, enslave_old_master=true)
      demoted = @master
      raise "Demoted node is already the master of this pool!" if demoted == promoted
      raise "Promoted host is not in the right pool!" unless demoted.slaves.include?(promoted)
      
      output "Preparing to demote master #{demoted} and promote #{promoted} in its place."
      live_promotion = demoted.running?
      
      # If demoted machine is available, confirm it is read-only and binlog isn't moving,
      # and then wait for slaves to catch up to this position. Or if using GTID, only need
      # to wait for new_master to catch up; GTID allows us to repoint lagging slaves without
      # issue.
      if live_promotion
        demoted.enable_read_only!
        raise "Unable to enable global read-only mode on demoted machine" unless demoted.read_only?
        raise "Demoted machine still taking writes (from superuser or replication?) despite being read-only" if taking_writes?(0.5)
        must_catch_up = (gtid_mode? ? [promoted] : demoted.slaves)

        must_catch_up.concurrent_each do |s|
          while demoted.ahead_of? s do
            s.output "Still catching up to demoted master"
            sleep 1
          end
        end
      
      # Demoted machine not available -- wait for slaves' binlogs to stop moving
      else
        demoted.slaves.concurrent_each do |s|
          while s.taking_writes?(1.0) do
            # Ensure we're not taking writes because a formerly dead master came back to life
            # In this situation, a human should inspect the old master manually
            raise "Dead master came back to life, aborting" if s.replicating?
            s.output "Still catching up on replication"
          end
        end
      end
      
      # Stop replication on all slaves
      replicas = demoted.slaves.dup
      replicas.each do |s|
        s.pause_replication if s.replicating?
      end
      raise "Unable to stop replication on all slaves" if replicas.any? {|s| s.replicating?}
      
      # Determine options for CHANGE MASTER
      creds = promoted.replication_credentials
      change_master_options = {
        user:     creds[:user],
        password: creds[:pass],
      }
      if gtid_mode?
        change_master_options[:auto_position] = true
        promoted.gtid_executed(true)
      else
        change_master_options[:log_file], change_master_options[:log_pos] = promoted.binlog_coordinates
      end
      
      # reset slave on promoted, and make sure read_only is disabled
      promoted.disable_replication!
      promoted.disable_read_only!
      
      # gather our new replicas
      replicas.delete promoted
      replicas << demoted if live_promotion && enslave_old_master
      
      # If old master is dead and we're using GTID, try to catch up the new master
      # from its siblings, in case one of them is further ahead. Currently using the
      # default 5-minute timeout of DB#replay_missing_transactions, gives up after that.
      if gtid_mode? && change_master_options[:auto_position] && !live_promotion
        promoted.replay_missing_transactions(replicas, change_master_options)
      end
      
      # Repoint replicas to the new master
      replicas.each {|r| r.change_master_to(promoted, change_master_options)}

      # ensure our replicas are configured correctly by comparing our staged values to current values of replicas
      promoted_replication_config = {
        master_host: promoted.ip,
        master_user: change_master_options[:user],
      }
      if gtid_mode?
        promoted_replication_config[:auto_position] = "1"
      else
        promoted_replication_config[:master_log_file] = change_master_options[:log_file]
        promoted_replication_config[:exec_master_log_pos] = change_master_options[:log_pos].to_s
      end
      replicas.each do |r|
        promoted_replication_config.each do |option, value|
          raise "Unexpected slave status value for #{option} in replica #{r} after promotion" unless r.slave_status[option] == value
        end
        r.resume_replication unless r.replicating?
      end
      
      # Update the pool
      # Note: if the demoted machine is not available, plugin may need to implement an
      # after_master_promotion! method which handles this case in configuration tracker
      @active_slave_weights.delete promoted # if promoting an active slave, remove it from read pool
      @master = promoted
      @master_uuid = nil # clear any memoized value
      sync_configuration
      Jetpants.topology.write_config
      
      output "Promotion complete. Pool master is now #{promoted}."
      replicas.all? {|r| r.replicating?}
    end


    def rolling_restart(reason)
      [standby_slaves, backup_slaves].each do |node|
        restart = 0
        reason.each do |r|
          value = node.global_variables[r.split('=').first.to_sym]
          if value != r.split('=').last
            restart += 1
          end
        end
        if restart > 0
          node.set_downtime 1
          # We do a fast restart here.
          node.enable_flush_innodb_cache = true
          node.restart_mysql
          node.catch_up_to_master if node.is_slave?
          node.cancel_downtime
        else
          output "No need to restart MySQL on #{node}, since the condition is already satisfied. #{reason}"
        end
      end
    end

    def slaves_layout
      {
        :standby_slave => Jetpants.standby_slaves_per_pool,
        :backup_slave  => Jetpants.backup_slaves_per_pool
      }
    end
    
    # Returns true if the entire pool is using gtid_mode AND has executed at least
    # one transaction with a GTID, false otherwise.
    # The gtid_executed check allows this method to tell when GTIDs can actually be
    # used (for auto-positioning, telling which node is ahead, etc) vs when we need
    # to fall back to using coordinates despite @@global.gtid_executed being ON.
    # This method is safe to use even if the master is dead.
    # In most situations, this method memoizes the value on first use, to avoid
    # repeated querying from subsequent calls.
    def gtid_mode?
      return @gtid_mode unless @gtid_mode.nil?
      any_gtids_executed = false
      # If master is running, it is sufficient to just check it alone, since
      # replicas must be using GTID if the master is
      nodes_to_examine = (master.running? ? [master] : slaves)
      nodes_to_examine.each do |db|
        begin
          row = db.query_return_first 'SELECT UPPER(@@global.gtid_mode) AS gtid_mode, @@global.gtid_executed AS gtid_executed'
        rescue
          # Treat pre-5.6 MySQL, or MariaDB, as not having GTID enabled. These will
          # raise an exception because the global vars in the query above don't exist.
          row = {gtid_mode: 'OFF', gtid_executed: ''}
        end
        unless row[:gtid_mode] == 'ON'
          @gtid_mode = false
          return @gtid_mode
        end
        any_gtids_executed = true unless row[:gtid_executed] == ''
      end
      if any_gtids_executed
        @gtid_mode = true
      else
        false # intentionally avoid memoization for this situation -- no way to invalidate properly
      end
    end
    
    # Returns the server_uuid of the pool's master. Safe to use even if the master is dead,
    # as long as the asset tracker populates the dead master's @slaves properly (as
    # jetpants_collins already does). Memoizes the value to avoid repeated lookup; methods
    # that modify the pool master clear the memoized value.
    def master_uuid
      return @master_uuid unless @master_uuid.nil?
      raise "Pool#master_uuid requires gtid_mode" unless gtid_mode?
      if master.running?
        @master_uuid = master.server_uuid
        return @master_uuid
      end
      slaves.select(&:running?).each do |s|
        if s.master == master
          master_uuid = s.slave_status[:master_uuid]
          unless master_uuid.nil? || master_uuid == ''
            @master_uuid = master_uuid
            return @master_uuid
          end
        end
      end
      raise "Unable to determine the master_uuid for #{self}"
    end
    
    # Informs your asset tracker about any changes in the pool's state or members.
    # Plugins should override this, or use before_sync_configuration / after_sync_configuration
    # callbacks, to provide an implementation of this method.
    def sync_configuration
    end
    
    # Callback to ensure that a sync'ed pool is already in Topology.pools
    def after_sync_configuration
      unless Jetpants.topology.pools.include? self
        Jetpants.topology.add_pool self
      end
    end
    
    # Returns the name of the pool.
    def to_s
      @name
    end

    # Jetpants::Pool proxies missing methods to the pool's @master Jetpants::DB instance.
    def method_missing(name, *args, &block)
      if @master.respond_to? name
        @master.send name, *args, &block
      else
        super
      end
    end
    
    def respond_to?(name, include_private=false)
      super || @master.respond_to?(name)
    end

    def slave_for_clone
      backup_slaves.empty? ? standby_slaves.last : backup_slaves.last
    end
    
  end
end
