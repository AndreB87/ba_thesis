defmodule FrameworkProvider.MnesiaConnection do
  @moduledoc """
  Connection to the mnesia database
  """

  alias FrameworkProvider.MessageModule, as: MessageModule

  @doc """
  Initializes the MnesiaServer used for the ServingLayer Server.
  Must be called once per Node.
  """
  def init(mnesia_server, ram_only \\true) do
    MnesiaServer.init(mnesia_server)
    table = :analytics
    columns = [:id, :temp, :hum, :station, :year, :month, :day, :elements, :temp_elements, :hum_elements]
    indices = [:station, :year, :month, :day]
    create_table(mnesia_server, table, columns, indices, ram_only)
  end

  @doc """
  Starts the MnesiaServer for the Serving Layer.

  ## Examples

      iex> ServingLayer.start()
      # {:ok, ServingLayerPID}

  """
  def start(mnesia_server, message_module, id, secret) do
    MessageModule.add_module(message_module, id, secret)
    mnesia_con = spawn(fn -> receive_loop(mnesia_server) end)
    spawn(fn -> recv_messages(message_module, mnesia_con, id, secret) end)
    {:ok, mnesia_con}
  end

  defp receive_loop(mnesia_server) do
    receive do
      {:analytics, :result, result} ->
        spawn(fn -> add_result(mnesia_server, :analytics, result) end)
        receive_loop(mnesia_server)
      {:get, :result, key, receiver} ->
        spawn(fn -> get_result(mnesia_server, key, receiver) end)
        receive_loop(mnesia_server)
      :kill ->
        MnesiaServer.stop(mnesia_server)
        :ok
      any ->
        IO.puts("Unrecognized Message: MNesia Connection Receive Loop")
        IO.puts(inspect(any))
        receive_loop(mnesia_server)
    end
  end

  defp add_result(mnesia_server, table, result) do
    Enum.each(Map.keys(result), fn key -> add_result(mnesia_server, table, key, Map.fetch!(result, key)) end)
  end

  defp add_result(mnesia_server, table, :avg_complete, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    station = 0
    year = 0
    month = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, :avg_complete, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, result_key, result) do
    Enum.each(
      Map.keys(result),
      fn key ->
        [sub_result] = Map.fetch!(result, key);
        add_result(mnesia_server, table, result_key, key, sub_result)
      end
    )
  end

  defp add_result(mnesia_server, table, :avg_year, year, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    station = 0
    month = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, year, temp, hum, station, {:year, year}, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, :avg_month, {year, month}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    station = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, {:month, year, month}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, :avg_day, {year, month, day}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    station = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, {:day, year, month, day}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, :avg_station_complete, station, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    year = 0
    month = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, {:station, station}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, :avg_station_year, {station, year}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    month = 0
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, {:station, station, year}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, :avg_station_month, {station, year, month}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    day = 0
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, {:station, station, year, month}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, :avg_station_day, {station, year, month, day}, result) do
    temp = result.avg_temp
    hum = result.avg_hum
    elements = result.elements
    temp_elements = result.temp_elements
    hum_elements = result.hum_elements
    add_result(mnesia_server, table, {:station, station, year, month, day}, temp, hum, station, year, month, day, elements, temp_elements, hum_elements)
  end

  defp add_result(mnesia_server, table, key, temp, hum, station, year, month, day, elements, temp_elements, hum_elements) do
    MnesiaServer.write(
      mnesia_server,
      table,
      [key, temp, hum, station, year, month, day, elements, temp_elements, hum_elements]
    )
  end

  defp get_result(mnesia_server, key, receiver) do
    result = get_single_result(mnesia_server, :analytics, key)
    send(receiver, {:result, result})
  end

  defp get_single_result(mnesia_server, table, key) do
    case MnesiaServer.read(mnesia_server, table, key) do
      {:error, error} ->
        {:error, error}
      {:ok, {_table, _key, avg_temp, avg_hum, station, year, month, day, elements, temp_elements, hum_elements}} ->
        %{
          avg_temp: avg_temp,
          avg_hum: avg_hum,
          elements: elements,
          temp_elements: temp_elements,
          hum_elements: hum_elements,
          station: station,
          year: year,
          month: month,
          day: day
        }
    end
  end

  defp create_table(mnesia_server, table, columns, indices, ram_only) do
    IO.puts inspect MnesiaServer.create_table(mnesia_server, table, columns, ram_only)
    Enum.each(indices, fn index -> MnesiaServer.create_index(mnesia_server, table, index) end)
  end

  defp recv_messages(message_module, mnesia_connection, id, secret) do
    case MessageModule.get_message(message_module, id, secret) do
      {:message, :no_message} ->
        recv_messages(message_module, mnesia_connection, id, secret)

      {:message, {:analytics, :result, result}} ->
        IO.puts("Add Result")
        send(mnesia_connection, {:analytics, :result, result})
        recv_messages(message_module, mnesia_connection, id, secret)

      {:message, :kill} ->
        IO.puts("Kill")
        send(mnesia_connection, :kill)

      any ->
        IO.puts("Unknown Message: MNesia Connection MessageModule Loop")
        IO.puts(inspect(any))
        recv_messages(message_module, mnesia_connection, id, secret)
    end
  end

end
