# This file contains any methods we're adding to core Ruby modules

# Add a deep_merge method to Hash in order to more effectively join configs
class Hash
  def deep_merge!(other_hash)
    merge!(other_hash) do |key, oldval, newval|
      (oldval.class == self.class && newval.class == oldval.class) ? oldval.deep_merge!(newval) : newval
    end
  end
end

# Reopen Enumerable to add some concurrent iterators
module Enumerable
  # Works like each but runs the block in a separate thread per item.
  def concurrent_each
    collect {|*item| Thread.new {yield *item}}.each {|th| th.join}
    self
  end
  
  # Works like map but runs the block in a separate thread per item.
  def concurrent_map
    collect {|*item| Thread.new {yield *item}}.collect {|th| th.value}
  end
  
  # Works like each_with_index but runs the block in a separate thread per item.
  def concurrent_each_with_index(&block)
    each_with_index.concurrent_each(&block)
  end
  
  # Alternative for concurrent_map which also has the ability to limit how
  # many threads are used. Much less elegant :(
  def limited_concurrent_map(thread_limit=40)
    lock = Mutex.new
    group = ThreadGroup.new
    items = to_a
    results = []
    pos = 0
    
    # Number of concurrent threads is the lowest of: self length, supplied thread limit, global concurrency limit
    [items.length, thread_limit, Jetpants.max_concurrency].min.times do
      th = Thread.new do
        while true do
          my_pos = nil
          lock.synchronize { my_pos = pos; pos += 1}
          break unless my_pos < items.length
          my_result = yield items[my_pos]
          lock.synchronize { results[my_pos] = my_result }
        end
      end
      group.add th
    end
    group.list.each {|th| th.join}
    results
  end
end

# Add Jetpants-specific conversion methods to Object.
class Object
  # Converts self to a Jetpants::Host by way of to_s. Only really useful for
  # Strings containing IP addresses, or Objects whose to_string method returns
  # an IP address as a string.
  def to_host
    Jetpants::Host.new(self.to_s)
  end
  
  # Converts self to a Jetpants::DB by way of to_s. Only really useful for
  # Strings containing IP addresses, or Objects whose to_string method returns
  # an IP address as a string.
  def to_db
    Jetpants::DB.new(self.to_s)
  end
end

class Range
  # Supply a block taking |chunk_min_id, chunk_max_id|. This will execute the block in
  # parallel chunks, supplying the min and max id (inclusive) of the current
  # chunk.  Note that thread_limit is capped at the value of Jetpants.max_concurrency.
  def in_chunks(chunks, thread_limit=40, min_per_chunk=1)
    per_chunk = ((max - min + 1) / chunks).ceil
    per_chunk = min_per_chunk if per_chunk < min_per_chunk

    min_id_queue = []
    results = []
    min.step(max, per_chunk) {|n| min_id_queue << n}
    min_id_queue.reverse!

    lock = Mutex.new
    group = ThreadGroup.new
    thread_count = Jetpants.max_concurrency
    thread_count = thread_limit if thread_limit && thread_limit < Jetpants.max_concurrency
    thread_count = min_id_queue.length if min_id_queue.length < thread_count
    thread_count.times do
      th = Thread.new do
        while min_id_queue.length > 0 do
          my_min = nil
          lock.synchronize {my_min = min_id_queue.pop}
          break unless my_min
          my_max = my_min + per_chunk - 1
          my_max = max if my_max > max
          result = yield my_min, my_max
          lock.synchronize {results << result}
        end
      end
      group.add(th)
    end
    group.list.each {|th| th.join}
    results
  end
end
