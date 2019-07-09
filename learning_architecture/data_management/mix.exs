defmodule DataManagement.MixProject do
  use Mix.Project

  def project do
    [
      app: :data_management,
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
      {:batch_processing, path: "../../batch_processing"},
      {:mnesia_server, path: "../../mnesia_server"}
    ]
  end
end