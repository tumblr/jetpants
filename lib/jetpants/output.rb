module Jetpants
  module Output
    def self.included(base)
      base.class_eval do
        define_method "output" do |str, table=nil, show_caller_file=false|
          str = str.to_s.strip
          str = nil if str && str.length == 0
          str ||= "Completed (no output)"
          output = Time.now.strftime("%H:%M:%S") + " [#{self}] "
          output << "in file #{caller[0]}" if show_caller_file
          output << table.name << ': ' if table
          output << str
          print output + "\n"
          output
        end
      end
    end
  end
end
