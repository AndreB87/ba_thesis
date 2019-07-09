# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

config :batch_processing,
  name: :batch_processing,
  node: :batch@e330,
  db_host: "172.17.0.2:9042"

config :mnesia_server,
  name: :mnesia_server,
  node: :mnesia@e330

config :stream_processing,
  name: :stream_processing,
  node: :stream@e330
