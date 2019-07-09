defmodule FrameworkProviderTest do
  use ExUnit.Case
  doctest FrameworkProvider

  test "greets the world" do
    assert FrameworkProvider.hello() == :world
  end
end
