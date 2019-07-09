defmodule MessageQueue do
  @moduledoc """
  Documentation for MessageQueue.
  """

  alias MessageQueue.Server, as: Server

  @doc """
  Hello world.

  ## Examples

      iex> MessageQueue.hello()
      :world

  """
  def start() do
    Server.start()
  end

  def stop(server) do
    send(server, {:kill, self()})
    get_result()
  end

  def add_module(server, id, secret) do
    send(server, {:add, :module, id, secret, self()})
    get_result()
  end

  def add_message(server, receiver, message) do
    send(server, {:add, :message, receiver, message, self()})
    get_result()
  end

  def get_message(server, receiver, secret) do
    send(server, {:get, :message, receiver, secret, self()})
    get_result()
  end

  defp get_result do
    receive do
      {:message_queue, result} ->
        result
      any ->
        IO.puts(inspect(any))
    end
  end

end
