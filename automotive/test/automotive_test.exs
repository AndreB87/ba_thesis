defmodule AutomotiveTest do
  use ExUnit.Case
  doctest Automotive

  test "greets the world" do
    assert Automotive.hello() == :world
  end
end
