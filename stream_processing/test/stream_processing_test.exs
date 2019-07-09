defmodule StreamProcessingTest do
  use ExUnit.Case
  doctest StreamProcessing

  test "greets the world" do
    assert StreamProcessing.hello() == :world
  end
end
