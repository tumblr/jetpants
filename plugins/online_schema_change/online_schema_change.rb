require 'json'

# load all the monkeypatches for other Jetpants classes, and our libraries
%w(pool db topology commandsuite ptosc percona-dsn).each {|mod| require "online_schema_change/lib/#{mod}"}

if Jetpants.plugin_enabled? 'jetpants_collins'
  require 'online_schema_change/lib/collins'
end
