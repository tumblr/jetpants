# JetCollins monkeypatches to add Collins integration

module Jetpants
  class Pool
    
    ##### JETCOLLINS MIX-IN ####################################################
    
    include Plugin::JetCollins
    
    # Used at startup time, to keep track of parent/child shard relationships
    attr_accessor :has_parent
    
    # Collins accessors for configuration asset metadata
    collins_attr_accessor :slave_pool_name, :aliases, :master_read_weight, :config_sort_order
    
    # Returns a Collins::Asset for this pool. Can optionally create one if not found.
    def collins_asset(create_if_missing=false)
      selector = {
        operation:    'and',
        details:      true,
        type:         'CONFIGURATION', 
        primary_role: 'MYSQL_POOL',
        pool:         "^#{@name.upcase}$",
        status:       'Allocated',
      }
      selector[:remoteLookup] = true if Jetpants.plugins['jetpants_collins']['remote_lookup']

      results = Plugin::JetCollins.find selector
      
      # If we got back multiple results, try ignoring the remote datacenter ones
      if results.count > 1
        filtered_results = results.select {|a| a.location.nil? || a.location.upcase == Plugin::JetCollins.datacenter}
        results = filtered_results if filtered_results.count > 0
      end
      
      if results.count > 1
        raise "Multiple configuration assets found for pool #{name}"
      elsif results.count == 0 && create_if_missing
        output "Could not find configuration asset for pool; creating now"
        new_tag = 'mysql-' + @name
        asset = Collins::Asset.new type: 'CONFIGURATION', tag: new_tag, status: 'Allocated'
        begin
          Plugin::JetCollins.create!(asset)
        rescue
          collins_set asset:  asset,
                      status: 'Allocated'
        end
        collins_set asset: asset, 
                    primary_role: 'MYSQL_POOL', 
                    pool: @name.upcase
        Plugin::JetCollins.get new_tag
      elsif results.count == 0 && !create_if_missing
        raise "Could not find configuration asset for pool #{name}"
      else
        results.first
      end
    end
    
    
    ##### METHOD OVERRIDES #####################################################
    
    # Examines the current state of the pool (as known to Jetpants) and updates
    # Collins to reflect this, in terms of the pool's configuration asset as
    # well as the individual hosts.
    def sync_configuration
      asset = collins_asset(true)
      collins_set asset: asset,
                  slave_pool_name: slave_name || '',
                  aliases: aliases.join(',') || '',
                  master_read_weight: master_read_weight
      [@master, slaves].flatten.each do |db|
        current_status = (db.collins_status || '').downcase
        db.collins_status = 'Allocated:RUNNING' unless current_status == 'maintenance'
        db.collins_pool = @name
      end
      @master.collins_secondary_role = 'MASTER'
      slaves(:active).each do |db| 
        db.collins_secondary_role = 'ACTIVE_SLAVE'
        weight = @active_slave_weights[db]
        db.collins_slave_weight = (weight == 100 ? '' : weight)
      end

      slaves(:standby).each {|db| db.collins_secondary_role = 'STANDBY_SLAVE'}
      slaves(:backup).each {|db| db.collins_secondary_role = 'BACKUP_SLAVE'}
      true
    end
    
    # If the pool's master hasn't been probed yet, return active_slaves list
    # based strictly on what we found in Collins. This is a major speed-up at
    # start-up time, especially for tasks that need to iterate over all pools.
    alias :active_slaves_from_probe :active_slaves
    def active_slaves
      if @master.probed?
        active_slaves_from_probe
      else
        @active_slave_weights.keys
      end
    end
    
    
    ##### CALLBACKS ############################################################
    
    # Pushes slave removal to Collins. (Normally this type of logic is handled by
    # Pool#sync_configuration, but that won't handle this case, since
    # sync_configuration only updates hosts still in the pool.)
    def after_remove_slave!(slave_db)
      slave_db.collins_pool = slave_db.collins_secondary_role = slave_db.collins_slave_weight = ''
      current_status = (slave_db.collins_status || '').downcase
      slave_db.collins_status = 'Unallocated' unless current_status == 'maintenance'
    end
    
    # If the demoted master was offline, record some info in Collins, otherwise
    # there will be 2 masters listed
    def after_master_promotion!(promoted, enslave_old_master=true)
      Jetpants.topology.clear_asset_cache
      
      # Find the master asset(s) for this pool, filtering down to only current datacenter
      assets = Jetpants.topology.server_node_assets(@name, :master)
      assets.reject! {|a| a.location && a.location.upcase != Plugin::JetCollins.datacenter}
      assets.map(&:to_db).each do |db|
        if db != @master || !db.running?
          db.collins_pool = ''
          db.collins_secondary_role = ''
          if enslave_old_master
            db.output 'REMINDER: you must manually put this host into Maintenance status in Collins' unless db.collins_status.downcase == 'maintenance'
          else
            db.collins_status = 'Unallocated'
          end
        end
      end
      
      # Clean up any slaves that are no longer slaving (again only looking at current datacenter)
      assets = Jetpants.topology.server_node_assets(@name, :slave)
      assets.reject! {|a| a.location && a.location.upcase != Plugin::JetCollins.datacenter}
      assets.map(&:to_db).each do |db|
        if !db.running? || db.pool != self
          db.output "Not replicating from new master, removing from pool #{self}"
          db.collins_pool = ''
          db.collins_secondary_role = ''
          db.collins_status = 'Unallocated'
        end
      end
    end
    
    
    ##### NEW METHODS ##########################################################
    
    # Returns the pool's creation time (as a unix timestamp) according to Collins.
    # (note: may be off by a few hours until https://github.com/tumblr/collins/issues/80
    # is resolved)
    # Not called from anything in jetpants_collins, but available to your own
    # custom automation if useful
    def collins_creation_timestamp
      collins_asset.created.to_time.to_i
    end
    
    # Called from DB#after_probe_master and DB#after_probe_slave for machines
    # that are unreachable via SSH, or reachable but MySQL isn't running.
    def slaves_according_to_collins
      results = []
      Jetpants.topology.server_node_assets(@name, :slave).each do |asset|
        slave = asset.to_db
        output "Collins found slave #{slave.ip} (#{slave.hostname})"
        results << slave
      end
      results
    end
    
  end
end