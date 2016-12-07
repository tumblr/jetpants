
module Jetpants
  class PreflightShardUI
    def initialize shards, concurrency=10
      @runner = PreflightShardRunner.new(shards, concurrency)
      @collector = ThreadsafeResultCollector.new
    end

    def on_each &block
      @block = block
    end

    def run!
      output "Will run on first shard (#{@runner.first_shard.name}), then prompt to continue\n\n"

      unless @runner.preflight(@collector, @block)
        output "Preflight shard failed!"
        summarize!
        return false
      end

      unless confirm!
        output "Abandoning attempt."
        return false
      end

      @runner.run_per_shard(@collector) do |shard,stage|
        output "--> #{shard}"
        @block.call(shard, stage)
      end

      summarize!
      return all_passed?
    end

    def confirm!
      continue = ask('First shard complete would you like to continue with the rest of the shards?:' +
                     '(YES/no) - YES has to be in all caps and fully typed')
      continue == 'YES'
    end

    def summarize!
      output return_summary
      output exception_summary.red
    end

    def all_passed?
      no_exceptions = @collector.exceptions.empty?
      all_succeeded = @collector.returns.values.all?

      return no_exceptions && all_succeeded
    end

    def return_summary
      return @collector.returns.reduce({}) { |keys_by_value,(key,value)|
        keys_by_value[value] ||= [];
        keys_by_value[value] << key;
        keys_by_value
      }.reduce([]) { |summary,(value, keys)|
        summary << "The following keys returned `#{value}':"
        summary.concat(keys.map { |k| " - #{k}" })
        summary
        }.join("\n")
    end

    def exception_summary
      return @collector.exceptions.reduce([]) { |summary,(key,exception)|
        summary << "#{exception.class.name} raised by `#{key}': #{exception}:"
        summary << "Backtrace:"
        summary.concat(
          exception.backtrace.map { |line| "\t#{line}" }
        )
      }.join("\n")
    end

    def output str, level = :info
      Jetpants.output(str, nil, level)
    end
  end

  class PreflightShardRunner
    attr_accessor :first_shard;

    def initialize shards, concurrency=10
      @shards = shards.dup
      @first_shard = @shards.shift
      @concurrency = concurrency
    end

    def preflight collector, block
      collector.run(@first_shard.name) {
        block.call(@first_shard, :preflight)
      }
    end

    def run_per_shard collector, &block
      @shards.limited_concurrent_map(@concurrency) do |shard|
        collector.run(shard.name) do
          block.call(shard, :all)
        end
      end

      collector
    end
  end
end
