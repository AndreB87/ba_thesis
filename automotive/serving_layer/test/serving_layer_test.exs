defmodule ServingLayerTest do
  use ExUnit.Case
  doctest ServingLayer

  test "greets the world" do
    assert ServingLayer.hello() == :world
  end
end
