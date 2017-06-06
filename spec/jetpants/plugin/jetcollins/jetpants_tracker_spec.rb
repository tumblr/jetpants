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
end
