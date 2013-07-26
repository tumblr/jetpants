module Jetpants
  
  class DB
    attr_accessor :aggregating_nodes

    def aggregator?
      return @aggregator unless @aggregator.nil?
      version_info = query_return_array('SHOW VARIABLES LIKE "%version%"')
      @aggregator = !version_info[:version_comment].nil? && version_info[:version_comment].downcase.include? "mariadb"
    end

    def aggregating_for?(node)
      return @aggregating_nodes && @aggregating_nodes.include? node
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

    def remove_aggregate_node(node)
      raise "Attempting to remove aggregate replication from an invalide node" unless node
      raise "Attempting to remove a node from a non-aggregate node" unless aggregate?
      raise "Attempting to remove a node that is not currently being aggregated" unless aggregating_for? node

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
        aggregate_pause_replication(*args)
        raise CallbackAbortError.new
      end
    end
    def aggregate_pause_replication(node)
      aggregate_repl_binlog_coordinates(node, true)
      if @replication_states[node] == :paused
        output "Aggregate replication was already paused."
      else
        output mysql_root_cmd "STOP SLAVE #{node}"
        @replication_states[node] = :paused
      end
    end

    def pause_all_replication
      raise "Pausing replication with no aggregating nodes" if @aggregating_nodes.empty?
      output "Pausing replication for #{@aggregating_nodes.join(" ")}"
      output mysql_root_cmd "STOP ALL SLAVES"
      @replicating_states.keys.each do |key|
        @replicating_states[key] = :paused
      end
    end

    def before_resume_replication(*args)
      if aggregator?
        aggregate_resume_replication(*args)
        raise CallbackAbortError.new
      end
    end
    def aggregate_resume_replication(node)
      raise "Attempting to resume aggregate replication for a node not in aggregate list" unless aggregating_for? node
      aggregate_repl_binlog_coordinates(node, true)
      output "Resuming aggregate replication from #{node}."
      output mysql_root_cmd "START SLAVE #{node}"
      @replication_states[node] = :running
    end

    def resume_all_replication
      raise "Resuming replication with no aggregating nodes" if @aggregating_nodes.empty?
      output "Resuming replication for #{@aggregating_nodes.join(" ")}"
      output mysql_root_cmd "START ALL SLAVES"
      @replicating_states.keys.each do |key|
        @replicating_states[key] = :running
      end
    end

    def before_pause_replication_with(*args)
      if aggregator?
        aggregate_pause_replication_with(*args)
        raise CallbackAbortError.new
      end
    end
    def aggregate_pause_replication_with
    end

    def before_disable_replication!(*args)
      if aggregator?
        # Don't use disable_replication! use remove_aggregate_node
        raise CallbackAbortError.new
      end
    end

    def before_repl_binlog_coordinates!(*args)
      if aggregator?
        aggregate_repl_binlog_coordinates!(*args)
        raise CallbackAbortError.new
      end
    end
    def aggregate_repl_binlog_coordinates(node, display_info=true)
      raise "This instance is not a slave" unless master
      status = aggregate_slave_status(node)
      file, pos = status[:relay_master_log_file], status[:exec_master_log_pos].to_i
      output "Has executed through master's binlog coordinates of (#{file}, #{pos})." if display_info
      [file, pos]
    end

    def before_seconds_behind_master!(*args)
      if aggregator?
        aggregate_seconds_behind_master!(*args)
        raise CallbackAbortError.new
      end
    end
    def aggregate_seconds_behind_master!
    end

    def before_catch_up_to_master!(*args)
      if aggregator?
        aggregate_catch_up_to_master!(*args)
        raise CallbackAbortError.new
      end
    end
    def aggregate_catch_up_to_master!
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
        message = "should be a slave of #{@master}, but SHOW SLAVE STATUS indicates otherwise"
        raise "#{self}: #{message}" if Jetpants.verify_replication
        output message
      end
      hash
    end

  end
end
