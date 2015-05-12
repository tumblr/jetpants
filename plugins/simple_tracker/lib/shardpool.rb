module Jetpants
  class ShardPool

    def sync_configuration
      Jetpants.topology.update_tracker_data
    end

    ##### NEW CLASS-LEVEL METHODS ##############################################

    # Converts a hash (from asset tracker json file) into a Shard.
    def self.from_hash(h)
      # we just return the shard for now... we have to wait until later to
      # set up children + parents, since it's easier to grab the corresponding
      # objects once all pools have been initialized.
      ShardPool.new(h['shard_pool'])
    end

    ##### NEW METHODS ##########################################################

    # Converts a Shard to a hash, for use in either the internal asset tracker
    # json (for_app_config=false) or for use in the application config file yaml 
    # (for_app_config=true)
    def to_hash(for_app_config = true)
      {
        shard_pool: @name
      }      
    end
  end
end
