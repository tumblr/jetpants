module Jetpants
  module Plugin
    module JetCollinsAsset

      class Tracker
        def initialize(asset, logger)
          @asset = asset
          @logger = logger
        end

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

        def set(attrs)
          asset = attrs[:asset] || @asset.call
          attrs.delete(:asset)
          upcase = !attrs[:literal]
          attrs.delete(:literal)

          if asset && asset.type.downcase == 'server_node' && asset.location && asset.location.upcase != Plugin::JetCollins.datacenter
            asset = nil unless jetcollins.inter_dc_mode?
          end

          unless asset
            output "WARNING: unable to set Collins attribute"
            return
          end
 
          if attrs.include? :state
            unless asset.status && attrs[:status]
              raise "#{self}: Unable to set state without settings a status" unless attrs[:status]
            end 
          end

          if attrs.include? :status
            val = attrs[:status] || ''
            state_val = attrs[:state]
            set_status_state(asset, val, state_val)
            attrs.delete(:status)
	    attrs.delete(:state)
          end

          attrs.each do |key, val|
            val ||= ''
            previous_value = asset.send(key)
            val = val.to_s
            val = val.upcase if upcase
            if previous_value != val
              success = jetcollins.set_attribute!(asset, key.to_s.upcase, val)
              raise "#{self}: Unable to set Collins attribute #{key} to #{val}" unless success
              if (val == '' || !val) && (previous_value == '' || !previous_value)
                false
              elsif val == ''
                output "Collins attribute #{key.to_s.upcase} removed (was: #{previous_value})"
              elsif !previous_value || previous_value == ''
                output "Collins attribute #{key.to_s.upcase} set to #{val}"
              else
                output "Collins attribute #{key.to_s.upcase} changed from #{previous_value} to #{val}"
              end
            end
          end
        end

        def set_status_state(asset, status, state)
          previous_status = asset.status.capitalize
          if status.include? ':'
            raise "Attempting to set state in two places" if state
            vals = status.split(':', 2)
            status = vals.first.capitalize
            state = vals.last.upcase
          end
          if state
            previous_state = asset.state.name.upcase
            if previous_state != state.to_s.upcase || previous_status != status.to_s.capitalize
              success = jetcollins.set_status!(asset, status, 'changed through jetpants', state)
              unless success
                jetcollins.state_create!(state, state, state, status)
                success = jetcollins.set_status!(asset, status, 'changed through jetpants', state)
              end
              raise "#{self}: Unable to set Collins state to #{state} and Unable to set Collins status to #{status}" unless success
              output "Collins status:state changed from #{previous_status}:#{previous_state} to #{status.capitalize}:#{state.upcase}"
            end
          elsif previous_status != status.to_s.capitalize
            success = jetcollins.set_status!(asset, status)
            raise "#{self}: Unable to set Collins status to #{status}" unless success
            output "Collins status changed from #{previous_status} to #{status}"
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
