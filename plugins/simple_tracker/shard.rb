module Jetpants
  class Shard < Pool
    
    ##### CALLBACKS ############################################################
    
    # After changing the state of a shard, sync config back to the asset tracker json
    def after_state=(value)
      sync_configuration
    end
    
    def after_cleanup!
      output 'This shard has now been fully split.'
      nodes.each do |n|
        n.output 'This node is no longer in use; please recycle or cancel it.'
      end
      puts 'If recycling nodes, be sure to completely clean them: wipe binlogs and all'
      puts 'MySQL data, and put clean data files with proper grants in place, before'
      puts 'you put the nodes back on the spare list.'
    end
    
    
    ##### NEW CLASS-LEVEL METHODS ##############################################
    
    # Converts a hash (from asset tracker json file) into a Shard.
    def self.from_hash(h)
      # we just return the shard for now... we have to wait until later to
      # set up children + parents, since it's easier to grab the corresponding
      # objects once all pools have been initialized.
      Shard.new(h['min_id'], h['max_id'], h['master'], h['state'].to_sym)
    end
    
    # Sets up parent/child relationships for the shard represented by the
    # supplied hash.
    def self.assign_relationships(h, all_shards)
      return unless h['parent']
      
      # figure out which shard corresponds to hash h
      min_id = h['min_id'].to_i
      max_id = (h['max_id'].to_s.upcase == 'INFINITY' ? 'INFINITY' : h['max_id'].to_i)
      shard = all_shards.select {|s| s.min_id == min_id && s.max_id == max_id}.first
      
      # now figure out which one is the parent, and assign parent/child relationship
      parent = all_shards.select {|s| s.name == h['parent']}.first
      raise "Cannot find parent shard #{h['parent']}" unless parent
      parent.add_child shard
    end
    
    
    ##### NEW METHODS ##########################################################
    
    # Converts a Shard to a hash, for use in either the internal asset tracker
    # json (for_app_config=false) or for use in the application config file yaml 
    # (for_app_config=true)
    def to_hash(for_app_config=false)

      if for_app_config
        # Ignore shards that shouldn't receive queries from the application
        return nil unless in_config?
        me = {'min_id' => min_id.to_i, 'max_id' => max_id == 'INFINITY' ? max_id : max_id.to_i}
        
        # We need to correctly handle child shards (which still have writes sent their parent),
        # read-only shards, and offline shards appropriately.
        return me.merge case state
                 when :ready, :needs_cleanup then {'host' => master.ip}
                 when :child then {'host_read' => master.ip, 'host_write' => master.master.ip}
                 when :read_only then {'host_read' => master.ip, 'host_write' => false}
                 when :offline then {'host' => false}
                 end
      else
        slave_data =  active_slave_weights.map {|db, weight| {'host' => db.to_s, 'weight' => weight, 'role' => 'ACTIVE_SLAVE'}} +
                      standby_slaves.map {|db| {'host' => db.to_s, 'role' => 'STANDBY_SLAVE'}} +
                      backup_slaves.map {|db| {'host' => db.to_s, 'role' => 'BACKUP_SLAVE'}}
        return {
          'min_id'    =>  min_id,
          'max_id'    =>  max_id,
          'parent'    =>  parent ? parent.to_s : nil,
          'state'     =>  state,
          'master'    =>  master,
          'slaves'    =>  slave_data,
        }
      end
    end
    
    
  end
end
