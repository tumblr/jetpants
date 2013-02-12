# JetCollins monkeypatches to add Collins integration

module Jetpants
  class DB
    
    ##### JETCOLLINS MIX-IN ####################################################
    
    include Plugin::JetCollins
    
    collins_attr_accessor :slave_weight
    
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
    
    
    ##### NEW METHODS ##########################################################
    
    # Returns true if this database is located in the same datacenter as jetpants_collins
    # has been figured for, false otherwise.
    def in_remote_datacenter?
      @host.collins_location != Plugin::JetCollins.datacenter
    end
    
    # Returns true if this database is a spare node and looks ready for use, false otherwise.
    # The default implementation just ensures a collins status of Provisioned.
    # Downstream plugins may override this to do additional checks to ensure the node is
    # in a sane state. (The caller of this method already checks that the node is SSHable,
    # and that MySQL is running, and the node isn't already in a pool -- so no need to
    # check any of those here.)
    def usable_spare?
      collins_status.downcase == 'provisioned'
    end
    
  end
end