require 'collins_client'

# Entrypoint for jetpants_collins plugin (namespace Jetpants::Plugin::JetCollins),
# which offers integration with the Collins hardware asset tracking system.
# This particular file accomplishes the following:
#
#   * Provides a JetCollins mixin module. Any class including this should also
#     implement a collins_asset method to convert objects to Collins assets;
#     the class can then use the provided collins_get and collins_set wrappers,
#     along with the collins_attr_accessor class method.
#
#   * Jetpants::Plugin::JetCollins can also be used as a global Collins API
#     client -- the module itself will delegate all missing methods to a
#     Collins::Client object.
#
#   * Loads monkeypatches for Jetpants classes DB, Host, Pool, Shard, Topology,
#     and Collins class Asset.
#
# Configuration options in Jetpants config file include:
#   user          =>  collins account username (required)
#   password      =>  collins account password (required)
#   url           =>  collins URL (required)
#   timeout       =>  collins client timeout, in seconds (default: 30)
#   datacenter    =>  collins data center name that we're running Jetpants in the context of (required if multi-datacenter)
#   remote_lookup =>  if true, supply remoteLookup parameter to search multiple datacenters (default: false)


module Jetpants
  module Plugin
    module JetCollins
      @collins_service = nil

      ##### CLASS METHODS ######################################################

      class << self
        include Output

        # We delegate missing class (module) methods to the collins API client,
        # if it responds to them.
        def method_missing(name, *args, &block)
          if service.respond_to? name
            Jetpants.with_retries(
                Jetpants.plugins['jetpants_collins']['retries'],
                Jetpants.plugins['jetpants_collins']['max_retry_backoff']
            ) {
              service.send name, *args, &block
            }
          else
            super
          end
        end

        def find(selector, retry_request = false, error_on_zero = true)
          if retry_request
            Jetpants.with_retries(
                Jetpants.plugins['jetpants_collins']['retries'],
                Jetpants.plugins['jetpants_collins']['max_retry_backoff']
            ) {
              res = service.send 'find', selector
              raise "Unable to find asset(s) for #{selector}" if (res.empty? && error_on_zero)
              return res
            }
          else
            service.send 'find', selector
          end
        end

        def count(selector, retry_request = false)
          if retry_request
            Jetpants.with_retries(
              Jetpants.plugins['jetpants_collins']['retries'],
              Jetpants.plugins['jetpants_collins']['max_retry_backoff']
            ) {
              res = service.send 'count', selector
              raise "Unable to find asset(s) for #{selector}" if res.nil?
              return res
            }
          else
            service.send 'count', selector
          end
        end

        # Eigenclass mix-in for collins_attr_accessor
        # Calling "collins_attr_accessor :foo" in your class body will create
        # methods collins_foo and collins_foo= which automatically get/set
        # Collins attribute foo
        def included(base)
          base.class_eval do
            def self.collins_attr_accessor(*fields)
              fields.each do |field|
                define_method("collins_#{field}") do
                  Jetpants.with_retries(
                      Jetpants.plugins['jetpants_collins']['retries'],
                      Jetpants.plugins['jetpants_collins']['max_retry_backoff']
                  ) {
                    (collins_get(field) || '').downcase
                  }
                end
                define_method("collins_#{field}=") do |value|
                  Jetpants.with_retries(
                      Jetpants.plugins['jetpants_collins']['retries'],
                      Jetpants.plugins['jetpants_collins']['max_retry_backoff']
                  ) {
                    result = collins_set(field, value)
                    Jetpants.with_retries(
                        Jetpants.plugins['jetpants_collins']['retries'],
                        Jetpants.plugins['jetpants_collins']['max_retry_backoff']
                    ) do
                      if field == :status && value.include?(':')
                        fetched = ("#{self.collins_status}:#{self.collins_state}").to_s.downcase
                      else
                        fetched = (collins_get(field) || '').to_s.downcase
                      end
                      expected = (value || '').to_s.downcase
                      if fetched != expected
                        raise "Retrying until Collins reports #{field} changed from '#{fetched}' to '#{expected}'."
                      end
                    end
                    result
                  }
                end
              end
            end

            # We make these 4 accessors available to ANY class including this mixin
            collins_attr_accessor :primary_role, :secondary_role, :pool, :status, :state
          end
        end

        # Returns the 'datacenter' config option for this plugin, or 'UNKNOWN-DC' if
        # none has been configured. This only matters in multi-datacenter Collins
        # topologies.
        def datacenter
          (Jetpants.plugins['jetpants_collins']['datacenter'] || 'UNKNOWN-DC').upcase
        end

        # Ordinarily, in a multi-datacenter environment, jetpants_collins places a number
        # of restrictions on interacting with assets that aren't in the local datacenter,
        # for safety's sake and to simplify how hierarchical replication trees are represented:
        #
        #   * Won't change Collins attributes on remote server node assets.
        #   * If a local node has a master in a remote datacenter, it is ignored/hidden.
        #   * If a local node has a slave in a remote datacenter, it's treated as a backup_slave,
        #     in order to prevent cross-datacenter master promotions. If any of these
        #     remote-datacenter slaves have slaves of their own, they're ignored/hidden.
        #
        # You may DISABLE these restrictions by calling enable_inter_dc_mode. Normally you
        # do NOT want to do this, except in special situations like a migration between
        # datacenters.
        def enable_inter_dc_mode
          Jetpants.plugins['jetpants_collins']['inter_dc_mode'] = true
          Jetpants.plugins['jetpants_collins']['remote_lookup'] = true
        end

        # Returns true if enable_inter_dc_mode has been called, false otherwise.
        def inter_dc_mode?
          Jetpants.plugins['jetpants_collins']['inter_dc_mode'] || false
        end

        def to_s
          Jetpants.plugins['jetpants_collins']['url']
        end

        def inspect
          to_s
        end

        private

        # Returns a Collins::Client object
        def service
          return @collins_service if @collins_service

          %w(url user password).each do |setting|
            raise "No Collins #{setting} set in plugins -> jetpants_collins -> #{setting}" unless Jetpants.plugins['jetpants_collins'][setting]
          end

          logger = Logger.new(STDOUT)
          logger.level = Logger::INFO
          config = {
            :host     =>  Jetpants.plugins['jetpants_collins']['url'],
            :timeout  =>  Jetpants.plugins['jetpants_collins']['timeout'] || 30,
            :username =>  Jetpants.plugins['jetpants_collins']['user'],
            :password =>  Jetpants.plugins['jetpants_collins']['password'],
            :logger   =>  logger,
          }
          @collins_service = Collins::Client.new(config)
        end
      end

      ##### INSTANCE (MIX-IN) METHODS ##########################################

      # The base class needs to implement this!
      def collins_asset
        raise "Any class including Plugin::JetCollins must also implement collins_asset instance method!"
      end

      def collins
        Jetpants::Plugin::JetCollinsAsset::Tracker.new(lambda { collins_asset }, lambda { |msg| output msg })
      end

      # Pass in a symbol, or array of symbols, to obtain from Collins for this
      # asset. For example, :status, :pool, :primary_role, :secondary_role.
      # If you pass in a single symbol, returns a single value.
      # If you pass in an array, returns a hash mapping each of these fields to their values.
      # Hash will also contain an extra field called :asset, storing the Collins::Asset object.
      def collins_get(*field_names)
        if field_names.count > 1 || field_names[0].is_a?(Array)
          field_names.flatten!
          return collins.get(field_names)
        elsif field_names.count == 1
          field_name = field_names[0]
          attributes = collins.get([field_name])
          return attributes[field_name]
        else
          return nil
        end
      end

      # Pass in a hash mapping field name symbols to values to set
      #   Symbol   => String         -- optionally set any Collins attribute
      #   :status  => String         -- optionally set the status value for the asset. Can optionally be a "status:state" string too.
      #   :asset   => Collins::Asset -- optionally pass this in to avoid an extra Collins API lookup, if asset already obtained
      #   :literal => Bool           -- optionally flag the value to not be upcased, only effective when setting attributes
      #
      # Alternatively, pass in 2 strings (field_name, value) to set just a single Collins attribute (or status)
      def collins_set(*args)
        collins.set(*args)
      end

      # Returns a single downcased "status:state" string, useful when trying to compare both fields
      # at once.
      def collins_status_state
        values = collins_get :status, :state
        "#{values[:status]}:#{values[:state]}".downcase
      end

    end # module JetCollins
  end # module Plugin
end # module Jetpants


# load all the monkeypatches for other Jetpants classes
%w(monkeypatch asset host db pool shard topology shardpool commandsuite jetpants_tracker).each {|mod| require "jetpants_collins/#{mod}"}
