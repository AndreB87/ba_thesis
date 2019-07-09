defmodule LearningArchitecture.MixProject do
  use Mix.Project

  def project do
    [
      app: :learning_architecture,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.19", only: :dev, runtime: false},
      {:data_gathering, path: "./data_gathering"},
      {:data_management, path: "./data_management"}
    ]
  end
end
