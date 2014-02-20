module Jetpants

  # This mixin defines an output function that prints the class, and optionally
  # caller file, method and line number if output_caller_info is set.
  module Output
    def output str, table=nil
      str = if str.nil? or str.is_a? String and str.length == 0
        "Completed (no output)"
      else
        str.to_s.strip
      end

      output = Time.now.strftime("%H:%M:%S") + " [#{self}] "
      output << "called from #{caller[0]} " if Jetpants.output_caller_info
      output << table.name << ': ' if table
      output << str
      print output + "\n"
      output
    end
  end
end
