defmodule ApplicationProvider.Orchestration do
  @moduledoc """
  Documentation for ApplicationProvider.
  """

  @doc """
  Initializes the Database Module
  """
  def init(ram_only \\true) do
    mnesia_server = {
      Application.get_env(:mnesia_server, :name),
      Application.get_env(:mnesia_server, :node)
    }
    FrameworkProvider.Database.init(mnesia_server, ram_only)
  end

  @doc """
  Hello world.
  """
  def start(folder) do
    {:ok, message_module} = FrameworkProvider.MessageModule.start()
    mnesia_server = {
      Application.get_env(:mnesia_server, :name),
      Application.get_env(:mnesia_server, :node)
    }
    batch_processing = {
      Application.get_env(:batch_processing, :name),
      Application.get_env(:batch_processing, :node)
    }
    {:ok, mnesia_con} = FrameworkProvider.Database.start(
      message_module,
      mnesia_server,
      batch_processing,
      :database_cas,
      :database_cas_secret,
      :database_mnesia,
      :database_mnesia_secret
    )
    ApplicationProvider.Analytics.start(
      message_module,
      batch_processing,
      :database_mnesia
    )
    ApplicationProvider.Preparation.start(
      message_module,
      :preparation,
      :preparation_secret,
      :database_cas
    )
    ApplicationProvider.Collection.start(
      message_module,
      :preparation,
      folder
    )
    {:ok, mnesia_con}
  end

end
