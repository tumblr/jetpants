require 'json'

# load all the monkeypatches for other Jetpants classes
%w(pool db topology commandsuite).each {|mod| require "online_schema_change/lib/#{mod}"}

require 'online_schema_change/lib/ptosc'
require 'online_schema_change/lib/percona-dsn'

if Jetpants.plugin_enabled? 'jetpants_collins'
  require 'online_schema_change/lib/collins'
end
