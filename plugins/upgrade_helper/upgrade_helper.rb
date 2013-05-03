# Entrypoint for upgrade_helper plugin

module Jetpants
  module Plugin
    module UpgradeHelper
      class << self
        # Shortcut for returning the configured new_version value
        def new_version
          Jetpants.plugins['upgrade_helper']['new_version']
        end
      end
    end
  end
end


# Verify mandatory config options
raise "No new_version specified in plugin config!" unless Jetpants::Plugin::UpgradeHelper.new_version

# load all the monkeypatches for other Jetpants classes
%w(pool shard host db commandsuite).each {|mod| require "upgrade_helper/#{mod}"}
