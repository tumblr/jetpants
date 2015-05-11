module Jetpants
  class Shard < Pool

    ##### CALLBACKS ############################################################

    # After changing the state of a shard, sync config back to the asset tracker json
    def after_state=(value)
      sync_configuration
    end

    ##### NEW CLASS-LEVEL METHODS ##############################################

    # Converts a hash (from asset tracker json file) into a Shard.
    def self.from_hash(h)
      # we just return the shard for now... we have to wait until later to
      # set up children + parents, since it's easier to grab the corresponding
      # objects once all pools have been initialized.
      Shard.new(h['shard_pool'])
    end

    def shards
      Jetpants.topology.shards(@name)
    end

    ##### NEW METHODS ##########################################################

    # Converts a Shard to a hash, for use in either the internal asset tracker
    # json (for_app_config=false) or for use in the application config file yaml 
    # (for_app_config=true)
    def to_hash
      {
        shard_pool: @name
      }      
    end
  end
end
