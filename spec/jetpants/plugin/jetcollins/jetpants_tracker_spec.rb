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
        allow(asset).to receive(:hi).and_return("there")
        f = StubAsset.new(asset)


        expect(f.collins_get('hi')).to eq("there")
      end
    end

    context "With multiple fields" do
      it "Returns a Hash of fields" do
        asset = double("MyAsset")
        allow(asset).to receive(:hi).and_return("there")
        allow(asset).to receive(:howdy).and_return("cowboy")
        f = StubAsset.new(asset)


        expect(f.collins_get(:hi, :howdy)).to eq({
                                                   asset: asset,
                                                   hi: "there",
                                                   howdy: "cowboy"
                                                 })
      end
    end
  end

  describe "#set" do
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


        expect(f.collins_set(hi: :there,
                             howdy: :cowboy)
              ).to eq(
                     hi: :there,
                     howdy: :cowboy
                   )
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


        expect(f.collins_set(hi: :there,
                             howdy: :cowboy,
                             asset: differentAsset)
              ).to eq(
                     asset: differentAsset,
                     hi: :there,
                     howdy: :cowboy
                   )
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


          expect(f.collins_set(
                   status: "Allocated:Running",
                   hi: :there,
                   howdy: :cowboy
                 )
                ).to eq(
                       hi: :there,
                       howdy: :cowboy,
                       status: "Allocated:Running"
                     )
        end
      end
    end
  end
end
