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

        def get(*field_names)
          asset = @asset.call

          if field_names.count > 1 || field_names[0].is_a?(Array)
            field_names.flatten!
            want_state = !! field_names.delete(:state)
            results = Hash[field_names.map {|field| [field, (asset ? asset.send(field) : '')]}]
            results[:state] = asset.state.name if want_state
            results[:asset] = asset
            results
          elsif field_names.count == 1
            return '' unless asset
            if field_names[0] == :state
              asset.state.name
            else
              asset.send field_names[0]
            end
          else
            nil
          end
        end

        def set(*args)
          attrs = (args.count == 1 ? args[0] : {args[0] => args[1]})
          asset = attrs[:asset] || @asset.call

          upcase = !attrs[:literal]
          attrs.delete(:literal)

          if asset && asset.type.downcase == 'server_node' && asset.location && asset.location.upcase != Plugin::JetCollins.datacenter
            asset = nil unless jetcollins.inter_dc_mode?
          end

          attrs.each do |key, val|
            val ||= ''
            case key
            when :asset
              next
            when :status
              unless asset
                output "WARNING: unable to set Collins status to #{val}"
                next
              end
              state_val = attrs[:state]
              previous_status = asset.status.capitalize
              if val.include? ':'
                raise "Attempting to set state in two places" if state_val
                vals = val.split(':', 2)
                val       = vals.first.capitalize
                state_val = vals.last.upcase
              end
              if state_val
                previous_state = asset.state.name.upcase
                next unless previous_state != state_val.to_s.upcase || previous_status != val.to_s.capitalize
                success = jetcollins.set_status!(asset, val, 'changed through jetpants', state_val)
                unless success
                  jetcollins.state_create!(state_val, state_val, state_val, val)
                  success = jetcollins.set_status!(asset, val, 'changed through jetpants', state_val)
                end
                raise "#{self}: Unable to set Collins state to #{state_val} and Unable to set Collins status to #{val}" unless success
                output "Collins status:state changed from #{previous_status}:#{previous_state} to #{val.capitalize}:#{state_val.upcase}"
              elsif previous_status != val.to_s.capitalize
                success = jetcollins.set_status!(asset, val)
                raise "#{self}: Unable to set Collins status to #{val}" unless success
                output "Collins status changed from #{previous_status} to #{val}"
              end
            when :state
              unless asset && asset.status && attrs[:status]
                raise "#{self}: Unable to set state without settings a status" unless attrs[:status]
                output "WARNING: unable to set Collins state to #{val}"
                next
              end
            else
              unless asset
                output "WARNING: unable to set Collins attribute #{key} to #{val}"
                next
              end
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
        end

        private
        def jetcollins
          Jetpants::Plugin::JetCollins
        end
      end
    end # module JetCollinsAsset
  end # module Plugin
end # module Jetpants
