require 'thread'

module Jetpants
  # Usage:
  #
  # t = Jetpants::ThreadsafeResultCollector.new
  # t.run("foo") { 1 }
  # t.run("bar") { 1 }
  # t.run("baz") { 3 }
  # t.run("tux") { raise "whut" }
  #
  # pp t.returns
  # pp t.exception

  class ThreadsafeResultCollector
    def initialize
      @semaphore = Mutex.new
      @returns = {}
      @exceptions = {}
    end

    def run key, &block
      record_return key, block.call()
    rescue => exception
      record_exception key, exception
      false
    end

    def record_exception key, e
      @semaphore.synchronize do
        @exceptions[key] = e
      end
    end

    def record_return key, ret
      @semaphore.synchronize do
        @returns[key] = ret
      end
    end

    def returns
      @semaphore.synchronize do
        return @returns.dup
      end
    end

    def exceptions
      @semaphore.synchronize do
        return @exceptions.dup
      end
    end
  end
end
