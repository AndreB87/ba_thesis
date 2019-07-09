defmodule FileReaderTest do
  use ExUnit.Case
  doctest FileReader

  test "greets the world" do
    assert FileReader.hello() == :world
  end
end
