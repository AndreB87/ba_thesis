defmodule MnesiaServerTest do
  use ExUnit.Case
  doctest MnesiaServer

  test "greets the world" do
    assert MnesiaServer.hello() == :world
  end
end
