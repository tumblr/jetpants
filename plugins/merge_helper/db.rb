module Jetpants
  
  class DB
    attr_accessor :aggregating_nodes

    def aggregator?
      return @aggregator unless @aggregator.nil?
      version_info = query_return_array('SHOW VARIABLES LIKE "%version%"')
      @aggregator = !version_info[:version_comment].nil? && version_info[:version_comment].downcase.include? "mariadb"
    end

    def aggregating_for?(node)
      return aggregator? && @aggregating_nodes && @aggregating_nodes.include? node
    end

    def add_node_to_aggregate(node, options_hash = {})
      raise "Attempting to add a node to aggregate to a non-aggregation node" unless aggregator?
      raise "Attempting to add an invalide aggregation source" unless node
      raise "Attempting to add a node that is already being aggregated" if aggreting_for? node

      @aggregating_nodes ||= []
      @replication_states ||= {}

      logfile = option_hash[:log_file]
      pos     = option_hash[:log_pos]
      if !(logfile && pos)
        raise "Cannot use coordinates of a new master that is receiving updates" if new_master.master && ! new_master.repl_paused?
        logfile, pos = new_master.binlog_coordinates
      end

      repl_user = option_hash[:user]     || replication_credentials[:user]
      repl_pass = option_hash[:password] || replication_credentials[:pass]

      result = mysql_root_cmd "CHANGE \"#{node}\" MASTER TO " +
        "MASTER_HOST='#{node.ip}', " +
        "MASTER_PORT=#{node.port}, " +
        "MASTER_LOG_FILE='#{logfile}', " +
        "MASTER_LOG_POS=#{pos}, " +
        "MASTER_USER='#{repl_user}', " +
        "MASTER_PASSWORD='#{repl_pass}'"

      output "Adding node #{node} to list of aggregation data sources with coordinates (#{logfile}, #{pos}). #{result}"
      @replication_states[node] = :paused
      @aggregating_nodes << node
      node.slaves << self
    end

    def remove_aggregate_node!(node)
      raise "Attempting to remove aggregate replication from an invalide node" unless node
      raise "Attempting to remove a node from a non-aggregate node" unless aggregate?
      raise "Attempting to remove a node that is not currently being aggregated" unless aggregating_for? node

      # Display the binlog coordinates in case we want to resume this stream at some point
      aggregate_repl_binlog_coordinates(node, true)
      output mysql_root_cmd "CHANGE \"#{node}\" MASTER TO master_user='test'; RESET SLAVE \"#{node}\""
      node.slaves.delete(self) rescue nil
      @replication_states[node] = nil
      @aggregating_nodes.delete(node)
    end

    def before_change_master_to(*args)
      # we don't use change_master_to on aggregate nodes, use add_node_to_aggregate
      raise CallbackAbortError.new if aggregator?
    end

    def before_pause_replication(*args)
      if aggregator?
        if !args.nil? && args.count > 0
          aggregate_pause_replication(*args)
        else
          pause_all_replication
        end
        raise CallbackAbortError.new
      end
    end
    def aggregate_pause_replication(node)
      raise "Attempting to pause aggregate replication from an invalid node" unless node
      aggregate_repl_binlog_coordinates(node, true)
      if @replication_states[node] == :paused
        output "Aggregate replication was already paused."
      else
        output mysql_root_cmd "STOP SLAVE \"#{node}\""
        @replication_states[node] = :paused
      end
      @repl_paused = !any_running_replication?
    end

    def pause_all_replication
      raise "Pausing replication with no aggregating nodes" if @aggregating_nodes.empty?
      output "Pausing replication for #{@aggregating_nodes.join(" ")}"
      output mysql_root_cmd "STOP ALL SLAVES"
      @replicating_states.keys.each do |key|
        @replicating_states[key] = :paused
      end
      @repl_paused = true
    end

    def before_resume_replication(*args)
      if aggregator?
        if !args.nil? && args.count > 0
          aggregate_resume_replication(*args)
        else
          resume_all_replication
        end
        raise CallbackAbortError.new
      end
    end
    def aggregate_resume_replication(node)
      raise "Attempting to resume aggregate replication for a node not in aggregation list" unless aggregating_for? node
      aggregate_repl_binlog_coordinates(node, true)
      output "Resuming aggregate replication from #{node}."
      output mysql_root_cmd "START SLAVE \"#{node}\""
      @replication_states[node] = :running
      @repl_paused = !any_running_replication?
    end

    # This is potentially dangerous, as it will start all replicating even if there are
    # some replication streams in a paused state
    def resume_all_replication
      raise "Resuming replication with no aggregating nodes" if @aggregating_nodes.empty?
      output "Resuming replication for #{@aggregating_nodes.join(", ")}"
      output mysql_root_cmd "START ALL SLAVES"
      @replicating_states.keys.each do |key|
        @replicating_states[key] = :running
      end
      @repl_paused = false
    end

    def before_pause_replication_with(*args)
      if aggregator?
        # We don't need this yet, add later if needed
        raise CallbackAbortError.new
      end
    end

    def before_disable_replication!(*args)
      if aggregator?
        # Don't use disable_replication! use remove_aggregate_node
        raise CallbackAbortError.new
      end
    end

    def before_repl_binlog_coordinates(*args)
      if aggregator?
        aggregate_repl_binlog_coordinates(*args)
        raise CallbackAbortError.new
      end
    end
    def aggregate_repl_binlog_coordinates(node, display_info=true)
      raise "Not performing aggregate replication for @{node}" unless aggregating_for? node
      status = aggregate_slave_status(node)
      file, pos = status[:relay_master_log_file], status[:exec_master_log_pos].to_i
      output "Has executed through master's binlog coordinates of (#{file}, #{pos})." if display_info
      [file, pos]
    end

    def before_seconds_behind_master(*args)
      if aggregator?
        aggregate_seconds_behind_master(*args)
        raise CallbackAbortError.new
      end
    end
    def aggregate_seconds_behind_master(node)
      raise "Not aggregate replicating #{node}" unless aggregating_for? node
      lag = aggregate_slave_status(node)[:seconds_behind_master]
      lag == 'NULL' ? nil : lag.to_i
    end

    def before_catch_up_to_master(*args)
      if aggregator?
        aggregate_catch_up_to_master(*args)
        raise CallbackAbortError.new
      end
    end
    # This is a lot of copypasta, punting on it for now until if/when we integrate more with core
    def aggregate_catch_up_to_master(node, timeout=3600, threshold=3, poll_frequency=5)
      raise "Attempting to catch up aggregate replication for a node which is not in the aggregation list" unless aggregating_for? node
      aggregate_resume_replication(node) if @replication_states[node] == :paused

      times_at_zero = 0
      start = Time.now.to_i
      output "Waiting to catch up to aggregation node"
      while (Time.now.to_i - start) < timeout
        lag = aggregate_seconds_behind_master(node)
        if lag == 0
          times_at_zero += 1
          if times_at_zero >= threshold
            output "Caught up to master."
            return true
          end
          sleep poll_frequency
        elsif lag.nil?
          aggregate_resume_replication(node)
          sleep 1
          raise "Unable to restart replication" if aggregate_seconds_behind_master(node).nil?
        else
          output "Currently #{lag} seconds behind master."
          times_at_zero = 0
          extra_sleep_time = (lag > 30000 ? 300 : (aggregate_seconds_behind_master(node) / 100).ceil)
          sleep poll_frequency + extra_sleep_time
        end
      end
      raise "This instance did not catch up to its aggregate data source \"#{node}\" within #{timeout} seconds"
    end

    def before_slave_status(*args)
      if aggregator?
        aggregate_slave_status(*args)
        raise CallbackAbortError.new
      end
    end
    def aggregate_slave_status(node)
      raise "Attempting to retrieve aggregate slave status for an invalid node" unless node
      raise "Attempting to retrieve aggregate slave status for a node which is not being aggregated" unless aggregating_for? node

      hash = mysql_root_cmd("SHOW SLAVE \"#{node}\"  STATUS", :parse=>true)
      hash = {} if hash[:master_user] == 'test'
      if hash.count < 1
        message = "Should be aggregating for #{node}, but SHOW SLAVE \"#{node}\" STATUS indicates otherwise"
        raise "#{self}: #{message}" if Jetpants.verify_replication
        output message
      end
      hash
    end

    # housekeeping on internal state
    def before_start_mysql(*args)
      if options.include?('--skip-slave-start')
        @replicating_states.keys.each do |key|
          @replicating_states[key] = :paused
        end
      end
    end

    def before_restart_mysql(*args)
      if options.include?('--skip-slave-start')
        @replicating_states.keys.each do |key|
          @replicating_states[key] = :paused
        end
      end
    end

    def any_running_replication?
      running = @replicating_states.select{ |state| state == :running }
      (running.count > 0)
    end

    def before_repl_paused?
      @repl_paused = !any_running_replication?
    end
  end
end
