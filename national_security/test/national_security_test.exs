defmodule NationalSecurityTest do
  use ExUnit.Case
  doctest NationalSecurity

  test "greets the world" do
    assert NationalSecurity.hello() == :world
  end
end
