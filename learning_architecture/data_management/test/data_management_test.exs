defmodule DataManagementTest do
  use ExUnit.Case
  doctest DataManagement

  test "greets the world" do
    assert DataManagement.hello() == :world
  end
end
