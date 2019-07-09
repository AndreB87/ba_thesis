defmodule DataGatheringTest do
  use ExUnit.Case
  doctest DataGathering

  test "greets the world" do
    assert DataGathering.hello() == :world
  end
end
