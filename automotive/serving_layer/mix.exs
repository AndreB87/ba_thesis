defmodule ServingLayer.MixProject do
  use Mix.Project

  def project do
    [
      app: :serving_layer,
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
      {:message_queue, path: "../message_queue"},
      {:frontend_layer, path: "../frontend_layer"},
      {:speed_layer, path: "../speed_layer"},
      {:batch_layer, path: "../batch_layer"},
      {:mnesia_server, path: "../../mnesia_server"}
    ]
  end
end
