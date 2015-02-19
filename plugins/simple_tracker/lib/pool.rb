module Jetpants
  class Pool
    
    ##### METHOD OVERRIDES #####################################################
    
    # This actually re-writes ALL the tracker json. With a more dynamic
    # asset tracker (something backed by a database, for example) this 
    # wouldn't be necessary - instead Pool#sync_configuration could just
    # update the info for the current pool (self) only.
    def sync_configuration
      Jetpants.topology.update_tracker_data
    end
    
    # If the pool's master hasn't been probed yet, return active_slaves list
    # based strictly on what we found in the asset tracker. This is a major 
    # speed-up at start-up time, especially for tasks that need to iterate 
    # over all pools' active slaves only, such as Topology#write_config.
    alias :active_slaves_from_probe :active_slaves
    def active_slaves
      if @master.probed?
        active_slaves_from_probe
      else
        @active_slave_weights.keys
      end
    end
    
    
    ##### NEW CLASS-LEVEL METHODS ##############################################
    
    # Converts a hash (from asset tracker json file) into a Pool.
    def self.from_hash(h)
      return nil unless h['master']
      p = Pool.new(h['name'], h['master'].to_db)
      p.master_read_weight = h['master_read_weight']
      p.slave_name = h['slave_name']
      h['aliases'].each {|a| p.has_alias a}
      h['slaves'].each do |slave_info|
        s = slave_info['host'].to_db
        p.has_active_slave(s, slave_info['weight']) if slave_info['role'] == 'ACTIVE_SLAVE'
      end
      p
    end
    
    
    ##### NEW METHODS ##########################################################
    
    # Converts a Pool to a hash, for use in either the internal asset tracker
    # json (for_app_config=false) or for use in the application config file yaml 
    # (for_app_config=true)
    def to_hash(for_app_config=false)
      if for_app_config
        slave_data = active_slave_weights.map {|db, weight| {'host' => db.to_s, 'weight' => weight}}
      else
        slave_data =  active_slave_weights.map {|db, weight| {'host' => db.to_s, 'weight' => weight, 'role' => 'ACTIVE_SLAVE'}} +
                      standby_slaves.map {|db| {'host' => db.to_s, 'role' => 'STANDBY_SLAVE'}} +
                      backup_slaves.map {|db| {'host' => db.to_s, 'role' => 'BACKUP_SLAVE'}}
      end
      
      {
        'name'                => name,
        'aliases'             => aliases,
        'slave_name'          => slave_name,
        'master'              => master.to_s,
        'master_read_weight'  => master_read_weight || 0,
        'slaves'              => slave_data
      }
    end
    
  end
end
