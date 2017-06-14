require 'jetpants'

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
end
