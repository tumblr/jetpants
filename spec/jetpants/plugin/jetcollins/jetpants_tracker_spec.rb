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
    # For debugging:
    # puts msg
  end

  def collins_asset
    @collins_asset
  end
end

RSpec.describe "JetCollinsCallingJetCollinsAssetTracker" do
  before {
    $JETCOLLINS = double("JetCollins")
  }

  describe "#get" do
    context "In the most trivial of fetchers" do
      it "does a direct attr send" do
        asset = double("MyAsset")
        expect(asset).to receive(:hi).and_return("there")
        f = StubAsset.new(asset)

        expect(f.collins_get('hi')).to eq("there")
      end
    end

    context "When passed multiple fields" do
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

    context "When passed args[0] as an array of fields" do
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

    context "When :state specified" do
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

    context "When asset specified as false, and requested multiple attributes" do
      it "Returns empty strings" do
        f = StubAsset.new(false)

        expect(f.collins_get(:hi, :howdy, :state)).to eq({
          asset: false,
          hi: "",
          howdy: "",
          state: ""
        })
      end
    end

    context "When asset specifed as false, and requested single attribute :state" do
      it "Returns an empty string" do
        f = StubAsset.new(false)

        expect(f.collins_get(:state)).to eq("")
      end
    end

    context "When requested single attribute :state" do
      it "Returns the STATE value" do
        asset = double("MyAsset")
        state = double("AssetState")
        expect(state).to receive(:name).and_return("Allocated")
        expect(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        expect(f.collins_get(:state)).to eq("Allocated")
      end
    end

    context "When requested no attributes" do
      it "Returns nil" do
        f = StubAsset.new(false)

        expect(f.collins_get()).to eq(nil)
      end
    end
  end

  describe "#set" do
    context "When attrs include asset" do
      it "is passed, rather than calling asset" do
        passed_asset = double("PassedAsset")
        original_asset = double("OriginalAsset")
        expect(passed_asset).to receive(:type).and_return("not a server asset")
        expect(passed_asset).to receive(:hi).and_return("there")

        original_stub_asset = StubAsset.new(original_asset)
 
        expect($JETCOLLINS).to receive(:set_attribute!).with(passed_asset, "HI", "THERE").and_return(true)
        original_stub_asset.collins_set({
          hi: :there,
          asset: passed_asset
        })
      end
    end

    context "When val provided is falsey" do
      it "returns empty string to set_attribute" do
        asset = double("MyAsset")
        allow(asset).to receive(:type).and_return("not a server asset")
        allow(asset).to receive(:hi).and_return("whatever")
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:set_attribute!).with(asset, "HI", "").and_return(true)

        expect(f.collins_set(hi: false)).to eq(hi: false)
      end
    end

    context "When asset is false" do
      it "skips setting status" do
        f = StubAsset.new(false)

        f.collins_set(:status, "Allocated")
      end
    end

    context "asset in another DC" do
      it "doesnt set value with set_attribute or set_status" do
        asset =  double("MyAsset")
        expect(asset).to receive(:type).and_return("server_node")
        expect(asset).to receive(:location).at_least(:once).and_return("earth2")
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:inter_dc_mode?).and_return(false)

        f.collins_set(:hi, "there")
      end
    end

    context "asset in another DC, inter_dc_mode? is TRUE" do
      it "sets attribute with set_attribute or set_status" do
        asset =  double("MyAsset")
        expect(asset).to receive(:type).and_return("server_node")
        expect(asset).to receive(:location).at_least(:once).and_return("earth2")
        expect(asset).to receive(:hi).and_return("HELLO")
        f = StubAsset.new(asset)

        $JETCOLLINS = double("JetCollins")
        expect($JETCOLLINS).to receive(:inter_dc_mode?).and_return(true)
        expect($JETCOLLINS).to receive(:set_attribute!).with(asset, "HI", "THERE").and_return(true)

        f.collins_set(:hi, "there")
      end
    end

    context "When passed key:value pair to collins_set" do
      it "does a direct attr send" do
        asset = double("MyAsset")
        allow(asset).to receive(:type).and_return("not a server asset :)")
        allow(asset).to receive(:hi).and_return("there")
        f = StubAsset.new(asset)

        expect($JETCOLLINS).to receive(:set_attribute!).with(asset, "HI", "THERE").and_return(true)

        expect(f.collins_set(:hi, :there)).to eq(hi: :there)
      end
    end

    context "When passed key:value pair to collins_set with existing value" do
      it "doesnt call set_attribute!" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:hi).and_return("THERE")
        f = StubAsset.new(asset)

        expect(f.collins_set(:hi, "there")).to eq(hi: "there")
      end
    end

    context "When setting the :status parameter" do
      it "does a basic set_status! if only the status is passed" do
        asset = double("MyAsset")
        allow(asset).to receive(:type).and_return("not a server asset")
        allow(asset).to receive(:status).and_return("Unallocated")
        f = StubAsset.new(asset)

        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated").and_return(true)

        f.collins_set(:status, "Allocated")
      end

      it "splits the status parameter if the state is also provided as appended to status" do
        asset = double("MyAsset")
        allow(asset).to receive(:type).and_return("not a server asset")
        allow(asset).to receive(:status).and_return("Unallocated")

        state = double("AssetState")
        expect(state).to receive(:name).and_return("Available")
        allow(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "RUNNING").and_return(true)

        f.collins_set(:status, "Allocated:Running")
      end

    context "When setting both :status and :state" do
      it "does a set_status! both status and state" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).at_least(:once).and_return("Unallocated")

        state = double("AssetState")
        expect(state).to receive(:name).and_return("Stopped")
        allow(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "Running").and_return(true)

        f.collins_set(state: "Running", status: "Allocated")
      end
    end

    context "When status:state matches previous value" do
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

    context "When :status is same in status:state" do
      it "splits the status parameter and sets the state" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset :)")
        expect(asset).to receive(:status).and_return("Allocated")

        state = double("AssetState")
        expect(state).to receive(:name).and_return("Available")
        expect(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "RUNNING").and_return(true)

        f.collins_set(:status, "Allocated:Running")
      end
    end

    context "When status part is same in status:state" do
      it "splits the status parameter and sets the state" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset :)")
        expect(asset).to receive(:status).and_return("Unallocated")

        state = double("AssetState")
        expect(state).to receive(:name).and_return("Running")
        expect(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "RUNNING").and_return(true)

        f.collins_set(:status, "Allocated:Running")
      end
    end

   context "When set_status! fails first-time for new :state attribute" do
      it "returns false, calls state_create! to add new :state and calls set_status! again" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).and_return("Unallocated")

        state = double("AssetState")
        expect(state).to receive(:name).and_return("What")
        expect(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "WHATEVER").and_return(false)
        expect($JETCOLLINS).to receive(:state_create!).with("WHATEVER", "WHATEVER", "WHATEVER", "Allocated").and_return(true)
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated", "changed through jetpants", "WHATEVER").and_return(true)

        f.collins_set(:status, "Allocated:Whatever")
      end
    end
    
    context "When set_status! fails first-time for new :status attribute" do
      it "returns false, calls state_create! to add new :status and calls set_status! again" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).and_return("What")

        state = double("AssetState")
        expect(state).to receive(:name).and_return("Spare")
        expect(asset).to receive(:state).and_return(state)
        f = StubAsset.new(asset)

        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Whatever", "changed through jetpants", "RUNNING").and_return(false)
        expect($JETCOLLINS).to receive(:state_create!).with("RUNNING", "RUNNING", "RUNNING", "Whatever").and_return(true)
        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Whatever", "changed through jetpants", "RUNNING").and_return(true)

        f.collins_set(:status, "Whatever:Running")
      end
    end
    
    context "When setting just the :status" do
      it "call set_status! to set the :status" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).and_return("Unallocated")

        f = StubAsset.new(asset)

        expect($JETCOLLINS).to receive(:set_status!).with(asset, "Allocated").and_return(true)

        f.collins_set(:status, "Allocated")
      end
    end
     
    context "When setting just the :status, with same existing value" do
      it "doesnt call set_status! as :status is same" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).and_return("Allocated")

        f = StubAsset.new(asset)

        f.collins_set(:status, "Allocated")
      end
    end
    
    context "When setting :state without passing :status" do
      it "raises an error" do
        asset = double("MyAsset")
        expect(asset).to receive(:type).and_return("not a server asset")
        expect(asset).to receive(:status).and_return("Allocated")

        f = StubAsset.new(asset)

        expect{f.collins_set(:state, "Running")}.to raise_error(RuntimeError)
      end
    end
    end

    context "When setting multiple attributes" do
      it "call set_attribute! multiple times" do
        asset = double("MyAsset")
        allow(asset).to receive(:type).and_return("not a server asset :)")
        allow(asset).to receive(:hi).and_return("there")
        allow(asset).to receive(:howdy).and_return("cowboy")
        f = StubAsset.new(asset)

        expect($JETCOLLINS).to receive(:set_attribute!).with(asset, "HI", "THERE").and_return(true)
        expect($JETCOLLINS).to receive(:set_attribute!).with(asset, "HOWDY", "COWBOY").and_return(true)

        f.collins_set(hi: :there, howdy: :cowboy)
      end
    end

    context "When setting multiple attributes to different asset" do
      it "call set_attribute! multiple times" do
        asset = double("MyAsset")
        f = StubAsset.new(asset)

        differentAsset = double("MyOtherAsset")
        allow(differentAsset).to receive(:type).and_return("not a server asset :)")
        allow(differentAsset).to receive(:hi).and_return("there")
        allow(differentAsset).to receive(:howdy).and_return("cowboy")

        expect($JETCOLLINS).to receive(:set_attribute!).with(differentAsset, "HI", "THERE").and_return(true)
        expect($JETCOLLINS).to receive(:set_attribute!).with(differentAsset, "HOWDY", "COWBOY").and_return(true)

        f.collins_set(
          hi: :there,
          howdy: :cowboy,
          asset: differentAsset
        )
      end
    end

    context "When setting a status:state parameter" do
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
