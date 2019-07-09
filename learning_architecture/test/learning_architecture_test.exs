defmodule LearningArchitectureTest do
  use ExUnit.Case
  doctest LearningArchitecture

  test "greets the world" do
    assert LearningArchitecture.hello() == :world
  end
end
