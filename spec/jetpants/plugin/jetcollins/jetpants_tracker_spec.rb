require 'jetpants'

module Jetpants
  module Plugin
    module JetCollinsAsset
      class Tracker
        def jetcollins
          $JETCOLLINS
        end
      end
    end
  end
end



class StubAsset
  include Jetpants::Plugin::JetCollins

  def initialize asset
    @collins_asset = asset
  end

  def output msg
    puts msg
  end

  def collins_asset
    @collins_asset
  end
end

RSpec.describe "JetCollinsCallingJetCollinsAssetTracker" do


  describe "#get" do
    context "In the most trivial of fetchers" do
      it "does a direct attr send" do
        asset = double("MyAsset")
        expect(asset).to receive(:hi).and_return("there")
        f = StubAsset.new(asset)

        expect(f.collins_get('hi')).to eq("there")
      end
    end

    context "With multiple fields" do
      it "Returns a Hash of fields" do
        asset = double("MyAsset")
        expect(asset).to receive(:hi).and_return("there")
        expect(asset).to receive(:howdy).and_return("cowboy")
        f = StubAsset.new(asset)


        expect(f.collins_get(:hi, :howdy)).to eq({
                                                   asset: asset,
                                                   hi: "there",
                                                   howdy: "cowboy"
                                                 })
      end
    end

    context "With args[0] as an array of fields" do
      it "Returns a Hash of fields" do
        asset = double("MyAsset")
        expect(asset).to receive(:hi).and_return("there")
        expect(asset).to receive(:howdy).and_return("cowboy")
        expect(asset).to receive(:foo).and_return("bar")
        f = StubAsset.new(asset)

        expect(f.collins_get([:hi, :howdy], :foo)).to eq({
          asset: asset, 
          hi: "there", 
          howdy: "cowboy", 
          foo: "bar"
        })
      end
    end

    context "With :state specified" do
      it "Returns a STATE value" do
        asset = double("MyAsset")
        expect(asset).to receive(:hi).and_return("there")
        expect(asset).to receive(:howdy).and_return("cowboy")
        state = double("AssetState")
        expect(state).to receive(:name).and_return("Allocated")
        expect(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)
        
        expect(f.collins_get(:hi, :howdy, :state)).to eq({
          asset: asset,
          hi: "there",
          howdy: "cowboy", 
          state: "Allocated"
        })
      end
    end

    context "With :state specified" do
      it "Returns a STATE value" do
        f = StubAsset.new(false)
        
        expect(f.collins_get(:hi, :howdy, :state)).to eq({
          asset: false,
          hi: "",
          howdy: "", 
          state: ""
        })
      end
    end

    context "With just :state specified and asset false" do
      it "Returns a empty string" do
        f = StubAsset.new(false)

        expect(f.collins_get(:state)).to eq("")
      end
    end

    context "With just :state specified" do
      it "Returns the asset STATE" do
        asset = double("MyAsset")
        state = double("AssetState")
        expect(state).to receive(:name).and_return("Allocated")
        expect(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        expect(f.collins_get(:state)).to eq("Allocated")
      end
    end

    context "When we request no attributes" do
      it "Returns nil" do
        f = StubAsset.new(false)

        expect(f.collins_get()).to eq(nil)
      end
    end
  end

  describe "#set" do
    context "attrs include asset" do
      it "is passed, rather than calling asset" do
        passed_asset = double("PassedAsset")
        original_asset = double("OriginalAsset")
        expect(passed_asset).to receive(:type).and_return("not a server asset")
        expect(passed_asset).to receive(:hi).and_return("there")

        original_stub_asset = StubAsset.new(original_asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_attribute!).with(passed_asset, "HI", "THERE").and_return(true)
        original_stub_asset.collins_set({
          hi: :there,
          asset: passed_asset
        })
      end
    end

    context "val provided is false/nil" do
      it "returns empty string" do
        asset = double("MyAsset")
        allow(asset).to receive(:type).and_return("not a server asset")
        allow(asset).to receive(:hi).and_return("whatever")
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_attribute!).with(asset, "HI", "").and_return(true)

        expect(f.collins_set(hi: false)).to eq(hi: false)
      end
    end

    context "asset is false" do
      it "skips setting status" do
        f = StubAsset.new(false)

        f.collins_set(:status, "Allocated")
      end
    end

    context "asset in another DC" do
      it "returns nil" do 
        asset =  double("MyAsset")
        expect(asset).to receive(:type).and_return("server_node")
        expect(asset).to receive(:location).at_least(:once).and_return("earth2")
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:inter_dc_mode?).and_return(false)

        f.collins_set(:status, "Allocated")
      end
    end

    context "asset in another DC, inter_dc_mode? true" do
      it "returns nil" do 
        asset =  double("MyAsset")
        expect(asset).to receive(:type).and_return("server_node")
        expect(asset).to receive(:location).at_least(:once).and_return("earth2")
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:inter_dc_mode?).and_return(true)

        f.collins_set(:status, "Allocated")
      end
    end

    context "In the most trivial of setters" do
      it "does a direct attr send" do
        asset = double("MyAsset")
        allow(asset).to receive(:type).and_return("not a server asset :)")
        allow(asset).to receive(:hi).and_return("there")
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_attribute!).with(asset, "HI", "THERE").and_return(true)

        expect(f.collins_set(:hi, :there)).to eq(hi: :there)
      end
    end

    context "Trying to set an attribute to existing value" do
      it "doesnt call set_attribute!" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:hi).and_return("THERE")
        f = StubAsset.new(asset)

        expect(f.collins_set(:hi, "there")).to eq(hi: "there")
      end
    end

    context "Setting the status parameter" do
      it "does a basic set if only the status is passed" do
        asset = double("MyAsset")
        allow(asset).to receive(:type).and_return("not a server asset :)")
        allow(asset).to receive(:status).and_return("Unallocated")
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated").and_return(true)

        expect(f.collins_set(:status, "Allocated")).to eq(status: "Allocated")
      end

      it "splits the status parameter if the state is also provided" do
        asset = double("MyAsset")
        allow(asset).to receive(:type).and_return("not a server asset :)")
        allow(asset).to receive(:status).and_return("Unallocated")

        state = double("AssetState")
        expect(state).to receive(:name).and_return("Available")
        allow(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "RUNNING").and_return(true)


        expect(f.collins_set(:status, "Allocated:Running")).to eq(status: "Allocated:Running")
      end

      # - passing :status and :state
    context "Pass both status and state" do
      it "sets both status and state" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).at_least(:once).and_return("Unallocated")

        state = double("AssetState")
        expect(state).to receive(:name).and_return("Stopped")
        allow(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "Running").and_return(true)

        f.collins_set(state: "Running", status: "Allocated")
      end
    end

      # - if the status:state does match previous value, make sure we don't set
    context " if status:state matches previous value" do
      it "does not call set_status!" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).at_least(:once).and_return("Allocated")
        f = StubAsset.new(asset)

        state = double("AssetState")
        expect(state).to receive(:name).and_return("Running")
        expect(asset).to receive(:state).and_return(state)

        f.collins_set(state: "Running", status: "Allocated")
      end
    end

      # - a test where the status is the same but the state is different
    context "if status is same in status:state" do
      it "splits the status parameter and sets the state" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset :)")
        expect(asset).to receive(:status).and_return("Allocated")

        state = double("AssetState")
        expect(state).to receive(:name).and_return("Available")
        expect(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "RUNNING").and_return(true)


        expect(f.collins_set(:status, "Allocated:Running")).to eq(status: "Allocated:Running")
      end
    end

      # - a test where the state is the same but the status is different
    context "if status is same in status:state" do
      it "splits the status parameter and sets the state" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset :)")
        expect(asset).to receive(:status).and_return("Unallocated")

        state = double("AssetState")
        expect(state).to receive(:name).and_return("Running")
        expect(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "RUNNING").and_return(true)


        expect(f.collins_set(:status, "Allocated:Running")).to eq(status: "Allocated:Running")
      end
    end

      # - where set_status! fails the first time (state doesn't exist, returns false),
      #   then make sure state_create is called, then set_status! passes (returns true)
      #   ^ this test is where you're setting a state and a status at the same time
   context "if set_status! fails first time for different :state" do
      it "returns false" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).and_return("Unallocated")

        state = double("AssetState")
        expect(state).to receive(:name).and_return("What")
        expect(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "WHATEVER").and_return(false)
        expect($JETCOLLINS).to receive(:state_create!).with("WHATEVER", "WHATEVER", "WHATEVER", "Allocated").and_return(true)
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "WHATEVER").and_return(true)

        f.collins_set(:status, "Allocated:Whatever")
      end
    end
      # - where set_status! fails the first time (status doesn't exist, returns false),
      #   then make sure state_create is called, then set_status! passes (returns false)
      #   ^ this test is where you're setting a state and a status at the same time
    context "if set_status! fails first time for different :status" do
      it "returns false" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).and_return("What")

        state = double("AssetState")
        expect(state).to receive(:name).and_return("Spare")
        expect(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Whatever", "changed through jetpants", "RUNNING").and_return(false)
        expect($JETCOLLINS).to receive(:state_create!).with("RUNNING", "RUNNING", "RUNNING", "Whatever").and_return(true)
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Whatever", "changed through jetpants", "RUNNING").and_return(true)

        f.collins_set(:status, "Whatever:Running")
      end
    end
      # - where you're setting just the status, but set_status! returns false
    context "if setting just the status" do
      it "returns false" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).and_return("Unallocated")

        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated").and_return(true)

        f.collins_set(:status, "Allocated")
      end
    end
      # - where you're setting just the status, but the status hasn't changed
     context "if setting just the status" do
      it "returns false" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).and_return("Allocated")

        f = StubAsset.new(asset)

        f.collins_set(:status, "Allocated")
      end
    end 
      # - when status is not passed and state is passed, raise an error
    context "if setting state without passing status" do
      it "raises an error" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).and_return("Allocated") 

        f = StubAsset.new(asset)

        expect{f.collins_set(:state, "Running")}.to raise_error(RuntimeError)
      end
    end
    end

    context "With multiple parameters" do
      it "sends many at once" do
        asset = double("MyAsset")
        allow(asset).to receive(:type).and_return("not a server asset :)")
        allow(asset).to receive(:hi).and_return("there")
        allow(asset).to receive(:howdy).and_return("cowboy")
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_attribute!).with(asset, "HI", "THERE").and_return(true)
        expect($JETCOLLINS).to receive(:set_attribute!).with(asset, "HOWDY", "COWBOY").and_return(true)


        f.collins_set(hi: :there, howdy: :cowboy)
      end
    end


    context "With multiple parameters" do
      it "sends many at once" do
        asset = double("MyAsset")
        f = StubAsset.new(asset)

        differentAsset = double("MyOtherAsset")
        allow(differentAsset).to receive(:type).and_return("not a server asset :)")
        allow(differentAsset).to receive(:hi).and_return("there")
        allow(differentAsset).to receive(:howdy).and_return("cowboy")



        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_attribute!).with(differentAsset, "HI", "THERE").and_return(true)
        expect($JETCOLLINS).to receive(:set_attribute!).with(differentAsset, "HOWDY", "COWBOY").and_return(true)


        f.collins_set(hi: :there,
                             howdy: :cowboy,
                             asset: differentAsset)
      end
    end

    context "with a status and state parameter" do
      it "splits the status parameter if the state is also provided" do
        asset = double("MyAsset")
        allow(asset).to receive(:type).and_return("not a server asset :)")
        allow(asset).to receive(:status).and_return("Unallocated")
        allow(asset).to receive(:hi).and_return("there")
        allow(asset).to receive(:howdy).and_return("cowboy")


        state = double("AssetState")
        expect(state).to receive(:name).and_return("Available")
        allow(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "RUNNING").and_return(true)
        expect($JETCOLLINS).to receive(:set_attribute!).with(asset, "HI", "THERE").and_return(true)
        expect($JETCOLLINS).to receive(:set_attribute!).with(asset, "HOWDY", "COWBOY").and_return(true)


        f.collins_set(
          status: "Allocated:Running",
          hi: :there,
          howdy: :cowboy
        )
      end
    end
  end
end
