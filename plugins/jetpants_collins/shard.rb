# JetCollins monkeypatches to add Collins integration

module Jetpants
  class Shard < Pool
    
    ##### JETCOLLINS MIX-IN ####################################################
    
    include Plugin::JetCollins
    
    collins_attr_accessor :shard_min_id, :shard_max_id, :shard_state, :shard_parent, :shard_pool
    
    # Returns a Collins::Asset for this pool
    def collins_asset(create_if_missing=false)
      selector = {
        operation:    'and',
        details:      true,
        type:         'CONFIGURATION', 
        primary_role: '^MYSQL_SHARD$',
        shard_min_id: "^#{@min_id}$",
        shard_max_id: "^#{@max_id}$",
        shard_pool:   "^#{@shard_pool.name}$"
      }
      selector[:remoteLookup] = true if Jetpants.plugins['jetpants_collins']['remote_lookup']
      
      results = Plugin::JetCollins.find selector, !create_if_missing
      
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
                    primary_role: 'MYSQL_SHARD', 
                    pool: @name.upcase,
                    shard_min_id: @min_id,
                    shard_max_id: @max_id,
                    shard_pool: @shard_pool.name.upcase
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
      status =  case
                when in_config? then 'Allocated'
                when @state == :deprecated then 'Cancelled'
                when @state == :recycle  then 'Decommissioned'
                else 'Provisioning'
                end
      collins_set asset: collins_asset(true),
                  status: status,
                  shard_state: @state.to_s.upcase,
                  shard_parent: @parent ? @parent.name : ''
      if @state == :recycle
        [@master, @master.slaves].flatten.each do |db|
          db.collins_pool = ''
          db.collins_secondary_role = ''
          db.collins_status = 'Unallocated'
        end
      elsif @state != :initializing
        # Note that we don't call Pool#slaves here to get all 3 types in one shot,
        # because that potentially includes child shards, and we don't want to overwrite
        # their pool setting...
        [@master, active_slaves, standby_slaves, backup_slaves].flatten.each do |db|
          current_status = (db.collins_status || '').downcase
          db.collins_status = 'Allocated:RUNNING' unless current_status == 'maintenance'
          db.collins_pool = @name
        end
        @master.collins_secondary_role = 'MASTER'

        standby_slaves.each {|db| db.collins_secondary_role = 'STANDBY_SLAVE'}
        backup_slaves.each  {|db| db.collins_secondary_role = 'BACKUP_SLAVE'}
      end
      
      # handle lockless master migration situations
      if @state == :child && master.master && !@parent
        to_be_ejected_master = master.master
        to_be_ejected_master.collins_secondary_role = :standby_slave # not accurate, but no better option for now
      end
      
      true
    end
    
    
    ##### CALLBACKS ############################################################
    
    # After altering the state of a shard, sync the change to Collins immediately.
    def after_state=(value)
      sync_configuration
    end
    
  end
end
