module Jetpants
  
  class Aggregator < DB
    include CallbackHandler

    def aggregating_nodes
      probe if @aggregating_node_list.nil?
      @aggregating_node_list
    end

    def initialize(ip, port=3306)
      super
      # we initialize master here prior to probing state due to the fact that the aggregator
      # uses a special set of multi-replication-stream functions and we don't track master state
      @master = false
    end

    # override the single master state probe with a probe of all replication sources
    def probe_master
      return unless running?
      raise "Attempting to probe a database without aggregation capabilities as an aggregate node" unless aggregator?
      probe_aggregate_nodes
    end

    def replication_states
      probe if @replication_states.nil?
      @replication_states
    end

    # uses multi-source replication semantics to build a list of replication sources
    def probe_aggregate_nodes
      @aggregating_node_list = []
      @replication_states = {}
      all_slave_statuses.each do |status|
        aggregate_node = DB.new(status[:master_host], status[:master_port])
        @aggregating_node_list << aggregate_node
        if status[:slave_io_running] != status[:slave_sql_running]
          output "One replication thread is stopped and the other is not for #{status[:name]}."
          if Jetpants.verify_replication
            output "You must repair this node manually, OR remove it from its pool permanently if it is unrecoverable."
            raise "Fatal replication problem on #{self}"
          end
          aggregate_pause_replication(aggregate_node)
          @replication_states[aggregate_node] = :paused
        else
          if status[:slave_io_running].downcase == 'yes'
            @replication_states[aggregate_node] = :running
          else
            @replication_states[aggregate_node] = :paused
          end
        end
      end 
    end

    def aggregating_for?(node)
      return aggregator? && aggregating_nodes && aggregating_nodes.include?(node)
    end

    def add_nodes_to_aggregate(nodes)
      nodes.each do |node|
        add_node_to_aggregate node
      end
    end

    # Similar to operations that change master
    # This method uses the aggregating node's pool as the connection name
    def add_node_to_aggregate(node, option_hash = {})
      raise "Attempting to add a node to aggregate to a non-aggregation node" unless aggregator?
      raise "Attempting to add an invalid aggregation source" unless node
      raise "Attempting to add a node that is already being aggregated" if aggregating_for? node

      @replication_states ||= {}

      logfile = option_hash[:log_file]
      pos     = option_hash[:log_pos]
      if !(logfile && pos)
        raise "Cannot use coordinates of a new master that is receiving updates" if node.master && ! node.repl_paused?
        logfile, pos = node.binlog_coordinates
      end

      repl_user = option_hash[:user]     || replication_credentials[:user]
      repl_pass = option_hash[:password] || replication_credentials[:pass]

      result = mysql_root_cmd "CHANGE MASTER '#{node.pool}' TO " +
        "MASTER_HOST='#{node.ip}', " +
        "MASTER_PORT=#{node.port}, " +
        "MASTER_LOG_FILE='#{logfile}', " +
        "MASTER_LOG_POS=#{pos}, " +
        "MASTER_USER='#{repl_user}', " +
        "MASTER_PASSWORD='#{repl_pass}'"

      output "Adding node #{node} to list of aggregation data sources with coordinates (#{logfile}, #{pos}). #{result}"
      @replication_states[node] = :paused
      @aggregating_node_list << node
      node.slaves << self
    end

    def remove_aggregate_node!(node)
      raise "Attempting to remove aggregate replication from an invalid node" unless node
      raise "Attempting to remove a node from a non-aggregate node" unless aggregator?
      raise "Attempting to remove a node that is not currently being aggregated" unless aggregating_for? node

      # Display the binlog coordinates in case we want to resume this stream at some point
      aggregate_repl_binlog_coordinates(node, true)
      output mysql_root_cmd "CHANGE MASTER '#{node.pool}' TO MASTER_USER='test'; RESET SLAVE '#{node.pool}' ALL"
      node.slaves.delete(self) rescue nil
      @replication_states[node] = nil
      @aggregating_node_list.select!{ |n| n != node }
    end

    def remove_all_nodes!
      nodes = aggregating_nodes.clone
      nodes.each do |node|
        remove_aggregate_node! node
      end
    end

    def change_master_to
      # we don't use change_master_to on aggregate nodes, use add_node_to_aggregate
      raise "Please use add_node_to_aggregate on aggregator nodes" if aggregator?
    end

    def pause_replication(*args)
      unless args.empty?
        aggregate_pause_replication(*args)
      else
        pause_all_replication
      end
    end

    def aggregate_pause_replication(node)
      raise "Attempting to pause aggregate replication from an invalid node" unless node
      raise "Attempting to pause aggregate replication for a node that is not currently being aggregated" unless aggregating_for? node
      if @replication_states[node] == :paused
        output "Aggregate replication was already paused."
        aggregate_repl_binlog_coordinates(node, true)
      else
        output mysql_root_cmd "STOP SLAVE '#{node.pool}'"
        aggregate_repl_binlog_coordinates(node, true)
        @replication_states[node] = :paused
      end
      @repl_paused = !any_replication_running?
    end

    # pauses replication from all sources, updating internal state
    def pause_all_replication
      raise "Pausing replication with no aggregating nodes" if aggregating_nodes.empty?
      replication_names = aggregating_nodes.map{|node| node.pool}.join(", ")
      output "Pausing replication for #{replication_names}"
      output mysql_root_cmd "STOP ALL SLAVES"
      @replication_states.keys.each do |key|
        @replication_states[key] = :paused
      end
      @repl_paused = true
    end

    def resume_replication(*args)
      unless args.empty?
        aggregate_resume_replication(*args)
      else
        resume_all_replication
      end
    end

    # resume replication from all sources, updating internal state
    def aggregate_resume_replication(node)
      raise "Attempting to resume aggregate replication for a node not in aggregation list" unless aggregating_for? node
      aggregate_repl_binlog_coordinates(node, true)
      output "Resuming aggregate replication from #{node.pool}."
      output mysql_root_cmd "START SLAVE '#{node.pool}'"
      @replication_states[node] = :running
      @repl_paused = !any_replication_running?
    end

    # This is potentially dangerous, as it will start all replicating even if there are
    # some replication streams in a paused state
    def resume_all_replication
      raise "Resuming replication with no aggregating nodes" if aggregating_nodes.empty?
      paused_nodes = replication_states.select{|node,state| state == :paused}.keys.map(&:pool)
      output "Resuming replication for #{paused_nodes.join(", ")}"
      output mysql_root_cmd "START ALL SLAVES"
      @replication_states.keys.each do |key|
        @replication_states[key] = :running
      end
      @repl_paused = false
    end

    def pause_replication_with(*args)
      raise "Aggregate node does not support this operation yet"
    end

    def before_disable_replication!(*args)
      raise "Please use remove_aggregate_node on an aggregator instance"
    end

    def repl_binlog_coordinates(*args)
      aggregate_repl_binlog_coordinates(*args)
    end

    def aggregate_repl_binlog_coordinates(node, display_info=true)
      raise "Not performing aggregate replication for #{node} (#{node.pool})" unless aggregating_for? node
      status = aggregate_slave_status(node)
      file, pos = status[:relay_master_log_file], status[:exec_master_log_pos].to_i
      output "Has executed through master (#{node})'s binlog coordinates of (#{file}, #{pos})." if display_info
      [file, pos]
    end

    def seconds_behind_master(*args)
      aggregate_seconds_behind_master(*args)
    end

    def aggregate_seconds_behind_master(node)
      raise "Not aggregate replicating #{node} (#{node.pool})" unless aggregating_for? node
      lag = aggregate_slave_status(node)[:seconds_behind_master]
      lag == 'NULL' ? nil : lag.to_i
    end

    def catch_up_to_master(*args)
      aggregate_catch_up_to_master(*args)
    end

    # This is a lot of copypasta, punting on it for now until if/when we integrate more with core
    def aggregate_catch_up_to_master(node, timeout=3600, threshold=3, poll_frequency=5)
      raise "Attempting to catch up aggregate replication for a node which is not in the aggregation list" unless aggregating_for? node
      aggregate_resume_replication(node) if replication_states[node] == :paused

      times_at_zero = 0
      start = Time.now.to_i
      output "Waiting to catch up to aggregation node"
      while (Time.now.to_i - start) < timeout
        lag = aggregate_seconds_behind_master(node)
        if lag == 0
          times_at_zero += 1
          if times_at_zero >= threshold
            output "Caught up to master \"#{node.pool}\" (#{node})."
            return true
          end
          sleep poll_frequency
        elsif lag.nil?
          aggregate_resume_replication(node)
          sleep 1
          raise "Unable to restart replication" if aggregate_seconds_behind_master(node).nil?
        else
          output "Currently #{lag} seconds behind master \"#{node.pool}\" (#{node})."
          times_at_zero = 0
          extra_sleep_time = (lag > 30000 ? 300 : (aggregate_seconds_behind_master(node) / 100).ceil)
          sleep poll_frequency + extra_sleep_time
        end
      end
      raise "This instance did not catch up to its aggregate data source \"#{node.pool}\" (#{node}) within #{timeout} seconds"
    end

    def slave_status(*args)
      unless args.empty?
        aggregate_slave_status(*args)
      else
        all_slave_statuses
      end
    end

    def aggregate_slave_status(node)
      raise "Attempting to retrieve aggregate slave status for an invalid node" unless node
      raise "Attempting to retrieve aggregate slave status for a node which is not being aggregated" unless aggregating_for? node

      hash = mysql_root_cmd("SHOW SLAVE '#{node.pool}'  STATUS", :parse=>true)
      hash = {} if hash[:master_user] == 'test'
      if hash.count < 1
        message = "Should be aggregating for #{node.pool}, but SHOW SLAVE '#{node.pool}' STATUS indicates otherwise"
        raise "#{self}: #{message}" if Jetpants.verify_replication
        output message
      end
      hash
    end

    def all_slave_statuses
      return unless running?
      status_strings = mysql_root_cmd("SHOW ALL SLAVES STATUS")
      return {} if status_strings.nil?

      # split on delimiter eg *************************** 3. row ***************************
      status_strings = status_strings.split(/\*{27} \d\. row \*{27}/)
      # for now we reset & set the slaving user to 'test' when destroying a replication stream, look to clear out later
      status_strings.map { |str| parse_vertical_result str }.select { |slave| !slave[:master_user].nil? && slave[:master_user] != 'test' }
    end

    # housekeeping on internal state
    def before_start_mysql(*options)
      if options.include?('--skip-slave-start')
        unless replication_states.nil?
          @replication_states.keys.each do |key|
            @replication_states[key] = :paused
          end
        end
      end
    end

    def before_restart_mysql(*options)
      if options.include?('--skip-slave-start')
        unless replication_states.nil?
          @replication_states.keys.each do |key|
            @replication_states[key] = :paused
          end
        end
      end
    end

    def any_replication_running?
      running = replication_states.select{ |name, state| state == :running }
      (running.count > 0)
    end

    def all_replication_running?
      running = replication_states.select{ |name, state| state == :running }
      (running.count == replication_states.count)
    end

    def before_repl_paused?
      @repl_paused = !any_replication_running?
    end

    def needs_cleanup?
      unless Jetpants.export_location.to_s.empty?
        num_out_files = ssh_cmd "ls -lh #{Jetpants.export_location}/*.out 2> /dev/null | wc -l"
        if num_out_files.to_i > 0
          output "Exported files seem to be present under #{Jetpants.export_location}"
          return true
        end
      end

      result = mysql_root_cmd("SELECT 1 FROM information_schema.user_privileges WHERE grantee LIKE '%jetpants%'")
      return false if result.nil?

      output "'jetpants' user exists on the db."
      true
    end

    def cleanup!
      unless Jetpants.export_location.to_s.empty?
        output "Cleaning up the Aggregator: #{Jetpants.export_location}/*"
        ssh_cmd "test -d #{Jetpants.export_location} && rm -f #{Jetpants.export_location}/*.out"
      end
      drop_user 'jetpants'
    end

    # Performs a validation step of pausing replication and determining row counts
    # on an aggregating server and its data sources
    # WARNING! This will pause replication on the nodes this machine aggregates from
    # And perform expensive row count operations on them
    def validate_aggregate_row_counts(restart_monitoring = true, tables = false)
      tables = Table.from_config('sharded_tables', aggregating_nodes.first.pool.shard_pool.name) unless tables
      query_nodes = [ slaves, aggregating_nodes ].flatten
      aggregating_nodes.concurrent_each do |node|
        node.disable_monitoring
        node.stop_query_killer
        node.pause_replication
      end
      begin
        node_counts = {}
        # gather counts for source nodes
        aggregating_nodes.concurrent_each do |node|
          counts = tables.limited_concurrent_map(8) { |table|
            rows = node.query_return_first_value("SELECT count(*) FROM #{table}")
            node.output "#{rows}", table
            [ table, rows ]
          }
          node_counts[node] = Hash[counts]
        end

        # wait until here to pause replication
        # to make sure all statements drain through
        slaves.concurrent_each do |node|
          node.disable_monitoring
          node.stop_query_killer
          node.pause_replication
        end

        # gather counts from slave
        # this should be the new shard master
        slave = slaves.last
        aggregate_counts = tables.limited_concurrent_map(8) { |table|
          rows = slave.query_return_first_value("SELECT count(*) FROM #{table}")
          slave.output "#{rows}", table
          [ table, rows ]
        }
        aggregate_counts = Hash[aggregate_counts]

        # sum up source node counts
        total_node_counts = {}
        aggregate_counts.keys.each do |key|
          total_node_counts[key] = 0
          aggregating_nodes.each do |node|
            total_node_counts[key] = total_node_counts[key] + node_counts[node][key]
          end
        end

        # validate row counts
        valid = true
        total_node_counts.each do |table,count|
          if total_node_counts[table] != aggregate_counts[table]
            valid = false
            output "Counts for #{table} did not match.  #{aggregate_counts[table]} on combined node and #{total_node_counts[table]} on source nodes"
          end
        end
      ensure
        if restart_monitoring
          query_nodes.concurrent_each do |node|
            node.start_replication
            node.catch_up_to_master
            node.start_query_killer
            node.enable_monitoring
          end
        end
      end
      if valid
        output "Row counts match"
      else
        output "Row count mismatch! check output above"
      end

      valid
    end
  end
end
