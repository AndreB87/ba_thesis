defmodule NationalSecurity.MixProject do
  use Mix.Project

  def project do
    [
      app: :national_security,
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
      {:application_provider, path: "./application_provider"},
      {:framework_provider, path: "./framework_provider"}
    ]
  end
end
