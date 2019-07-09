defmodule BpTestTest do
  use ExUnit.Case
  doctest BpTest

  test "greets the world" do
    assert BpTest.hello() == :world
  end
end
