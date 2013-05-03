require 'json'


# load all the monkeypatches for other Jetpants classes
%w(pool db topology commandsuite).each {|mod| require "online_schema_change/#{mod}"}