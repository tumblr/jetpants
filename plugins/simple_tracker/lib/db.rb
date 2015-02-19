module Jetpants
  class DB

    def is_spare?
      Jetpants.topology.spares.include? self
    end

    def claim!
      spares = Jetpants.topology.spares.reject do |sp|
        self == (sp.is_a?(Hash) && sp['node'] ? sp['node'].to_db : sp.to_db)
      end

      Jetpants.topology.tracker.spares = spares
      Jetpants.topology.update_tracker_data
    end

    ##### CALLBACKS ############################################################
    
    # Determine master from asset tracker if machine is unreachable or MySQL isn't running.
    def after_probe_master
      unless @running
        my_pool, my_role = Jetpants.topology.tracker.determine_pool_and_role(@ip, @port)
        @master = (my_role == 'MASTER' ? false : my_pool.master)
      end
    end
    
    # Determine slaves from asset tracker if machine is unreachable or MySQL isn't running
    def after_probe_slaves
      unless @running
        @slaves = Jetpants.topology.tracker.determine_slaves(@ip, @port)
      end
    end
    
  end
end

