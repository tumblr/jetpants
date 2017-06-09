module Jetpants
  module Plugin
    module JetCollinsAsset

      class Tracker
        def initialize(asset, logger)
          @asset = asset
          @logger = logger
        end

        attr_reader :asset

        def output msg
          @logger.call msg
        end

        def get(field_names)
          asset = @asset.call

          want_state = !! field_names.delete(:state)
          results = Hash[field_names.map {|field| [field, (asset ? asset.send(field) : '')]}]
          results[:state] = (asset ? asset.state.name : '') if want_state
          results[:asset] = asset
          results
        end

        def set(asset, status, state, attr=false)
 
          if (status && state) && !attr
            jetcollins.set_status!(asset, status, 'changed through jetpants', state)
          elsif (status && !state) && !attr
            jetcollins.set_status!(asset, status)
          elsif attr
            key = status
            value = state
            jetcollins.set_attribute!(asset, key, value)
          end
        end

        private
        def jetcollins
          Jetpants::Plugin::JetCollins
        end
      end
    end # module JetCollinsAsset
  end # module Plugin
end # module Jetpants
