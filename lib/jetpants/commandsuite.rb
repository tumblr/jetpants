require 'thor'

module Jetpants
  class CommandSuite < Thor

    no_tasks do
      def output(str = "\s", level = :info)
        Jetpants.output str, nil, level
      end
    end

  end
end
