defmodule FrontendLayerTest do
  use ExUnit.Case
  doctest FrontendLayer

  test "greets the world" do
    assert FrontendLayer.hello() == :world
  end
end
