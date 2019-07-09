defmodule FrameworkProvider.MessageModule do
  @moduledoc """
  Holds Messages in a queue and distribute them for the processes
  """

  @doc """
  Starts the Message Module
  """
  def start do
    {:ok, spawn(fn -> loop(%{}) end)}
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

  defp loop(mq_data) do
    receive do
      {:add, :module, id, secret, pid} ->
        add_module(mq_data, id, secret, pid)
        |> loop()
      {:add, :message, receiver, message, pid} ->
        add_message(mq_data, receiver, message, pid)
        |> loop()
      {:get, :message, id, secret, pid} ->
        get_message(mq_data, id, secret, pid)
        |> loop()
      {:kill, pid} ->
        send(pid, {:message_queue, :ok})
    end
  end

  defp add_module(mq_data, id, secret, pid) do
    if Map.has_key?(mq_data, id) do
      send(pid, {:message_queue, {:error, :already_in}})
      mq_data
    else
      send(pid, {:message_queue, :ok})
      Map.put_new(mq_data, id, {secret, []})
    end
  end

  defp add_message(mq_data, receiver, message, pid) do
    if Map.has_key?(mq_data, receiver) do
      send(pid, {:message_queue, :ok})
      {secret, messages} = Map.fetch!(mq_data, receiver)
      %{mq_data | receiver => {secret, messages ++ [message]}}
    else
      send(pid, {:message_queue, {:error, :enoent}})
      mq_data
    end
  end

  defp get_message(mq_data, id, secret, pid) do
    case Map.fetch(mq_data, id) do
      :error ->
        send(pid, {:message_queue, {:error, :enoent}})
        mq_data
      {:ok, {saved_secret, messages}} ->
        if saved_secret === secret do
          case messages do
            [] ->
              send(pid, {:message_queue, {:message, :no_message}})
              mq_data
            [message | tail] ->
              send(pid, {:message_queue, {:message, message}})
              %{mq_data | id => {secret, tail}}
          end
        else
          send(pid, {:message_queue, {:error, :wrong_secret}})
          mq_data
        end
    end
  end

end
