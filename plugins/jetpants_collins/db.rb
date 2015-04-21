# JetCollins monkeypatches to add Collins integration

module Jetpants
  class DB
    
    ##### JETCOLLINS MIX-IN ####################################################
    
    include Plugin::JetCollins
    
    collins_attr_accessor :slave_weight, :nodeclass, :nobackup

    # Because we only support 1 mysql instance per machine for now, we can just
    # delegate this over to the host
    def collins_asset
      @host.collins_asset
    end
    
    
    ##### METHOD OVERRIDES #####################################################
    
    # Add an actual collins check to confirm a machine is a standby
    def is_standby?
      !(running?) || (is_slave? && !taking_connections? && collins_secondary_role == 'standby_slave')
    end
    
    # Treat any node outside of current data center as being for backups.
    # This prevents inadvertent cross-data-center master promotion.
    def for_backups?
      hostname.start_with?('backup') || in_remote_datacenter?
    end

    def is_spare?
      collins_status_state.to_s.downcase == 'allocated:spare'
    end

    def claim!
      self.collins_pool = ''
      self.collins_secondary_role = ''
      self.collins_slave_weight = ''
      self.collins_status = 'Allocated:CLAIMED'
      self
    end

    def return_to_spare!
      self.collins_status = 'Allocated:Spare'
    end

    def clone_settings_to!(*targets)
      clone_attributes = Jetpants.plugins['jetpants_collins']['clone_attributes'] || []
      clone_attributes.each do |attribute|
        targets.each do |target|
          target.collins_set attribute, self.collins_get(attribute)
        end
      end
    end

    ##### CALLBACKS ############################################################
    
    # Determine master from Collins if machine is unreachable or MySQL isn't running.
    def after_probe_master
      unless @running
        if collins_secondary_role == 'master'
          @master = false
        else
          pool = Jetpants.topology.pool(collins_pool)
          @master = pool.master if pool
        end
      end
      
      # We completely ignore cross-data-center master unless inter_dc_mode is enabled.
      # This may change in a future Jetpants release, once we support tiered replication more cleanly.
      if @master && @master.in_remote_datacenter? && !Jetpants::Plugin::JetCollins.inter_dc_mode?
        @remote_master = @master # keep track of it, in case we need to know later
        @master = false
      elsif !@master
        in_remote_datacenter? # just calling to cache for current node, before we probe its slaves, so that its slaves don't need to query Collins
      end
    end
    
    # Determine slaves from Collins if machine is unreachable or MySQL isn't running
    def after_probe_slaves
      # If this machine has a master AND has slaves of its own AND is in another data center,
      # ignore its slaves entirely unless inter_dc_mode is enabled.
      # This may change in a future Jetpants release, once we support tiered replication more cleanly.
      @slaves = [] if @running && @master && @slaves.count > 0 && in_remote_datacenter? && !Jetpants::Plugin::JetCollins.inter_dc_mode?
      
      unless @running
        p = Jetpants.topology.pool(self)
        @slaves = (p ? p.slaves_according_to_collins : [])
      end
    end
    
    # After changing the status of a node, clear its list of spare-node-related
    # validation errors, so that we will re-probe when necessary
    def after_collins_status=(value)
      @spare_validation_errors = nil
    end

    ##### NEW METHODS ##########################################################

    # Returns true if this database is located in the same datacenter as jetpants_collins
    # has been figured for, false otherwise.
    def in_remote_datacenter?
      @host.collins_location != Plugin::JetCollins.datacenter
    end

    # override in a custom plugin to parse location info in a specific environment
    # returning relevant location information to parse in pool::location_map
    # ex: { dc: dc,row: row, position: position }
    def location_hash
    end

    # checks to see if a db is usable with another
    def usable_with?(db)
      true
    end

    # checks ot see if a db is usable within a pool
    def usable_in?(pool)
      true
    end

    # checks the physical location of a database compared to another
    def is_near?(db)
      false
    end

    # used for sorting spares for preference and considering physical locality
    # the higher the proximity score the less it will be preferred
    def proximity_score(pool)
      0
    end

    # Returns the Jetpants::Pool that this instance belongs to, if any.
    # Can optionally create an anonymous pool if no pool was found. This anonymous
    # pool intentionally has a blank sync_configuration implementation.  Rely on
    # Collins for pool information if it is already in one.
    def pool(create_if_missing=false)
      result = Jetpants.topology.pool(collins_pool || self)

      if !result && master
        result = Jetpants.topology.pool(master)
      elsif !result && create_if_missing
        pool_master = master || self
        result = Pool.new('anon_pool_' + pool_master.ip.tr('.', ''), pool_master)
        def result.sync_configuration; end
      end
      result
    end

  end
end
