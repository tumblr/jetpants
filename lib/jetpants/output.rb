require 'logger'
require 'colored'

module Jetpants

  attr_accessor :output_lock
  attr_accessor :output_logger

  # This mixin defines an output function that prints the class, and optionally
  # caller file, method and line number if output_caller_info is set.
  module Output
    @@output_lock = nil
    @@output_logger = nil

    def output(str = "\s", table = nil, level = :info)
      str = if str.nil? or (str.is_a? String and str.length == 0)
              "Completed (no output)"
            else
              str.to_s.strip
            end

      output = ''
      output << "[#{self.to_s.blue}] " unless self.to_s == 'console'
      output << "called from #{caller[0]} " if Jetpants.output_caller_info
      output << table.name << ': ' if table
      output << str

      unless Jetpants.log_file.to_s.empty?
        context = self.to_s == 'console' ? 'console' : self.class.name
        @@output_logger ||= Logger.new(Jetpants.log_file)
        @@output_logger.send(level, context) {
          output
            .encode('UTF-8', :undef => :replace, :invalid => :replace, :replace => '')
            .gsub(/\e\[\d+(;\d+)*m/, '') # remove all coloring from Highline
            .gsub(/^#{level}: /i, '')    # remove repetition of the logging level
        }
      end

      # add the current time for display purposes
      output = [Time.now.strftime("%H:%M:%S"), output].join(' ')
      (@@output_lock ||= Mutex.new).synchronize {
        puts output
      }

      true
    end
  end

end
