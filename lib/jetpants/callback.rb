module Jetpants
  # Exception class used to halt further processing in callback chain.  See
  # description in CallbackHandler.
  class CallbackAbortError < StandardError; end
  
  # If you include CallbackHandler as a mix-in, it grants the base class support
  # for Jetpants callbacks, as defined here:
  #
  # If you invoke a method "foo", Jetpants will first 
  # automatically call any "before_foo" methods that exist in the class or its
  # superclasses. You can even define multiple methods named before_foo (in the
  # same class!) and they will each be called. In other words, Jetpants
  # callbacks "stack" instead of overriding each other.
  # 
  # After calling any/all before_foo methods, the foo method is called, followed
  # by all after_foo methods in the same manner.
  #
  # If any before_foo method raises a CallbackAbortError, subsequent before_foo
  # methods will NOT be called, NOR will foo itself nor any after_foo methods.
  #
  # If any after_foo method raises a CallbackAbortError, subsequent after_foo
  # methods will NOT be called.
  #
  # You may preceed the definition of a callback method with "callback_priority 123"
  # to set an explicit priority (higher = called first) for subsequent callbacks.
  # The default priority is 100.
  module CallbackHandler
    def self.included(base)
      base.class_eval do
        class << self
          # Set the priority (higher = called first) for any subsequent callbacks defined in the current class.
          def callback_priority(value)
            @callback_priority = value
          end

          def method_added(name)
            # Intercept before_* and after_* methods and create corresponding Callback objects
            if name.to_s.start_with? 'before_', 'after_'
              Callback.new self, name.to_s.split('_', 2)[1].to_sym, name.to_s.split('_', 2)[0].to_sym, @callback_priority
            
            # Intercept redefinitions of methods we've already wrapped, so we can
            # wrap them again
            elsif Callback.wrapped? self, name
              Callback.wrap_method self, name
            end
          end
        end
        
        # Default priority for callbacks is 100
        @callback_priority = 100
      end
    end
  end
  
  # Generic representation of a before-method or after-method callback.
  # Used internally by CallbackHandler; you won't need to interact with Callback directly.
  class Callback
    @@all_callbacks = {}        # hash of class obj -> method_name symbol -> type string -> array of callbacks
    @@currently_wrapping = {}   # hash of class obj -> method_name symbol -> bool
    
    attr_reader :for_class    # class object
    attr_reader :method_name  # symbol containing method name (the one being callback-wrapped)
    attr_reader :type         # :before or :after
    attr_reader :priority     # high numbers get triggered first
    attr_reader :my_alias     # method name alias OF THE CALLBACK
    
    def initialize(for_class, method_name, type=:after, priority=100)
      @for_class = for_class
      @method_name = method_name
      @type = type
      @priority = priority
      
      @@all_callbacks[for_class] ||= {}
      @@all_callbacks[for_class][method_name] ||= {}
      already_wrapped = Callback.wrapped?(for_class, method_name)
      @@all_callbacks[for_class][method_name][type] ||= []

      next_method_id = @@all_callbacks[for_class][method_name][type].count + 1
      old_name = "#{type.to_s}_#{method_name.to_s}".to_sym
      @my_alias = new_name = ("real_callback_#{old_name}_" + for_class.to_s.sub('::', '_') + "_#{next_method_id}").to_sym
      for_class.class_eval do
        alias_method new_name, old_name
      end
      Callback.wrap_method(for_class, method_name) unless already_wrapped
      
      @@all_callbacks[for_class][method_name][type] << self
    end
    
    def self.wrap_method(for_class, method_name)
      @@currently_wrapping[for_class] ||= {}
      @@currently_wrapping[for_class][method_name] ||= false
      return if @@currently_wrapping[for_class][method_name] # prevent infinite recursion from the alias_method call
      @@currently_wrapping[for_class][method_name] = true
      
      for_class.class_eval do
        alias_method "#{method_name}_without_callbacks".to_sym, method_name
        define_method method_name do |*args|
          begin
            Callback.trigger(self, method_name, :before, *args)
          rescue CallbackAbortError
            return
          end
          result = send "#{method_name}_without_callbacks".to_sym, *args
          begin
            Callback.trigger(self, method_name, :after, *args)
          rescue CallbackAbortError
          end
          result
        end
      end
      
      @@currently_wrapping[for_class][method_name] = false
    end
    
    def self.trigger(for_object, method_name, type, *args)
      my_callbacks = []
      for_object.class.ancestors.each do |for_class|
        if @@all_callbacks[for_class] && @@all_callbacks[for_class][method_name] && @@all_callbacks[for_class][method_name][type]
          my_callbacks.concat(@@all_callbacks[for_class][method_name][type])
        end
      end
      my_callbacks.sort_by! {|c| -1 * c.priority}
      my_callbacks.each {|c| for_object.send(c.my_alias, *args)}
    end
    
    def self.wrapped?(for_class, method_name)
      return false unless @@all_callbacks[for_class] && @@all_callbacks[for_class][method_name]
      @@all_callbacks[for_class][method_name].count > 0
    end
  end
end
