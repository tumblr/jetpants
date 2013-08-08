# load all the monkeypatches for other Jetpants classes
%w(db commandsuite aggregator).each {|mod| require "merge_helper/lib/#{mod}"}
