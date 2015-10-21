# Adds conversion methods to the Collins::Asset class for obtaining Jetpants equivalents

module Collins
  class Asset
    
    # Convert a Collins::Asset to a Jetpants::DB. Requires asset TYPE to be SERVER_NODE.
    def to_db
      raise "Can only call to_db on SERVER_NODE assets, but #{self} has type #{type}" unless type.upcase == 'SERVER_NODE'
      backend_ip_address.to_db
    end
    
    
    # Convert a Collins::Asset to a Jetpants::Host. Requires asset TYPE to be SERVER_NODE.
    def to_host
      raise "Can only call to_host on SERVER_NODE assets, but #{self} has type #{type}" unless type.upcase == 'SERVER_NODE'
      backend_ip_address.to_host
    end

    # Convert a Collins:Asset to a Jetpants::ShardPool
    def to_shard_pool
      raise "Can only call to_shard_pool on CONFIGURATION assets, but #{self} has type #{type}" unless type.upcase == 'CONFIGURATION'
      raise "Unknown primary role #{primary_role} for configuration asset #{self}" unless primary_role.upcase == 'MYSQL_SHARD_POOL'
      raise "No shard_pool attribute set on asset #{self}" unless shard_pool && shard_pool.length > 0

      Jetpants::ShardPool.new(shard_pool)
    end

    # Convert a Collins::Asset to either a Jetpants::Pool or a Jetpants::Shard, depending
    # on the value of PRIMARY_ROLE.  Requires asset TYPE to be CONFIGURATION.
    def to_pool
      raise "Can only call to_pool on CONFIGURATION assets, but #{self} has type #{type}" unless type.upcase == 'CONFIGURATION'
      raise "Unknown primary role #{primary_role} for configuration asset #{self}" unless ['MYSQL_POOL', 'MYSQL_SHARD'].include?(primary_role.upcase)
      raise "No pool attribute set on asset #{self}" unless pool && pool.length > 0
      
      # if this node is iniitalizing we know there will be no server assets
      # associated with it
      if !shard_state.nil? and shard_state.upcase == "INITIALIZING"
        master_assets = []
      else
        # Find the master(s) for this pool. If we got back multiple masters, first
        # try ignoring the remote datacenter ones
        master_assets = Jetpants.topology.server_node_assets(pool.downcase, :master)
      end

      if master_assets.count > 1
        results = master_assets.select {|a| a.location.nil? || a.location.upcase == Plugin::JetCollins.datacenter}
        master_assets = results if results.count > 0
      end
      puts "WARNING: multiple masters found for pool #{pool}; using first match" if master_assets.count > 1
      
      if master_assets.count == 0
        puts "WARNING: no masters found for pool #{pool}; ignoring pool entirely"
        result = nil
      
      elsif primary_role.upcase == 'MYSQL_POOL'
        result = Jetpants::Pool.new(pool.downcase, master_assets.first.to_db)
        if aliases
          aliases.split(',').each {|a| result.add_alias(a.downcase)}
        end
        result.slave_name = slave_pool_name if slave_pool_name
        result.master_read_weight = master_read_weight if master_read_weight

        # We intentionally only look for active slaves in the current datacenter, since we
        # treat other datacenters' slaves as backup slaves to prevent promotion or cross-DC usage
        active_slave_assets = Jetpants.topology.server_node_assets(pool.downcase, :active_slave)
        active_slave_assets.reject! {|a| a.location && a.location.upcase != Plugin::JetCollins.datacenter}
        active_slave_assets.each do |asset|
          weight = asset.slave_weight && asset.slave_weight.to_i > 0 ? asset.slave_weight.to_i : 100
          result.has_active_slave(asset.to_db, weight)
        end
        
      elsif primary_role.upcase == 'MYSQL_SHARD'
        result = Jetpants::Shard.new(shard_min_id.to_i, 
                                   shard_max_id == 'INFINITY' ? 'INFINITY' : shard_max_id.to_i, 
                                   master_assets.first.to_db, 
                                   shard_state.downcase.to_sym,
                                   shard_pool)
        
        # We'll need to set up the parent/child relationship if a shard split is in progress,
        # BUT we need to wait to do that later since the shards may have been returned by
        # Collins out-of-order, so the parent shard object might not exist yet.
        # For now we just remember the NAME of the parent shard.
        result.has_parent = shard_parent if shard_parent
        
      else
        raise "Unknown configuration asset primary role #{primary_role} for asset #{self}"
      end
      
      result
    end
    
  end
end
