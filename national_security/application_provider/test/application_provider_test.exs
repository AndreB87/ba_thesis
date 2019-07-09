defmodule ApplicationProviderTest do
  use ExUnit.Case
  doctest ApplicationProvider

  test "greets the world" do
    assert ApplicationProvider.hello() == :world
  end
end
