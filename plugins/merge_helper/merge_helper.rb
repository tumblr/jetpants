# load all the monkeypatches for other Jetpants classes
%w(db commandsuite).each {|mod| require "merge_helper/#{mod}"}
