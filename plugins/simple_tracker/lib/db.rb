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

    def cleanup_spare!
      # If the node is already a valid spare, do not do anything
      return true if probe! && usable_spare?

      if running?
        datadir = mysql_root_cmd('select @@datadir;').chomp("\n/")
        mysql_root_cmd("PURGE BINARY LOGS BEFORE NOW();") rescue nil
      else
        datadir = '/var/lib/mysql'
      end

      stop_mysql
      output "Initializing the MySQL data directory"
      ssh_cmd [
        "rm -rf #{datadir}/*",
        '/usr/bin/mysql_install_db'
      ], 1

      output service(:start, 'mysql')
      confirm_listening
      @running = true

      usable_spare?
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

