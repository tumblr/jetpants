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
    # Normally no need for plugins to override this (as of Jetpants 0.8.1), they should 
    # override DB#validate_spare instead.
    def usable_spare?
      if @spare_validation_errors.nil?
        @spare_validation_errors = []
        @spare_validation_errors << 'Node already has a pool' if pool
        @spare_validation_errors << 'Node is not reachable via SSH' unless available?
        @spare_validation_errors << 'MySQL is not running' unless running?
        validate_spare
        if @spare_validation_errors.size > 0
          error_text = @spare_validation_errors.join '; '
          output "Removed from spare pool for failing checks: #{error_text}"
        end
      end
      @spare_validation_errors.size == 0
    end
    
    # Performs validation checks on this node to see whether it is a usable spare.
    # The default implementation just ensures a collins status of Provisioned.
    # Downstream plugins may override this to do additional checks to ensure the node is
    # in a sane state.
    # No need to check whether the node is SSHable, MySQL is running, or not already in
    # a pool -- DB#usable_spare? already does that automatically.
    def validate_spare
      # Confirm still in provisioned state. (Because Collins find API hits a search index
      # which isn't synchronously updated with all writes, there's potential for a find
      # call to return assets that just transitioned out of provisioned.)
      @spare_validation_errors << "Unexpected status value: #{collins_status}" unless collins_status.downcase == 'provisioned'
    end
  end
end