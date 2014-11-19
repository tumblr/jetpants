require 'logger'

module Jetpants

  # This mixin defines an output function that prints the class, and optionally
  # caller file, method and line number if output_caller_info is set.
  module Output
    def output(str = "\s", table = nil, level = :info)
      str = if str.nil? or (str.is_a? String and str.length == 0)
              "Completed (no output)"
            else
              str.to_s.strip
            end

      output = ''
      output << "[#{self.pool}] " if self.is_a?(Jetpants::Pool) or self.is_a?(Jetpants::DB)
      output << "[#{self}] " unless self.to_s == 'console'
      output << "called from #{caller[0]} " if Jetpants.output_caller_info
      output << table.name << ': ' if table
      output << str

      log = Logger.new(Jetpants.log_file)
      log.send(level, output)

      # add the current time for display purposes
      output = [Time.now.strftime("%H:%M:%S"), output].join(' ')

      puts output
      output
    end
  end

end
