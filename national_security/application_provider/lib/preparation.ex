defmodule ApplicationProvider.Preparation do
  @moduledoc """
  The Preparation Module transforms the data from the
  collection module to make it usefull for the next
  steps in analyzing the data
  """

  alias FrameworkProvider.MessageModule, as: MessageModule

  def start(message_module, id, secret, data_receiver) do
    MessageModule.add_module(message_module, id, secret)
    spawn(fn -> loop(message_module, data_receiver, id, secret) end)
  end

  defp loop(message_module, data_receiver, id, secret) do
    case MessageModule.get_message(message_module, id, secret) do
      {:message, :no_message} ->
        loop(message_module, data_receiver, id, secret)

      {:message, {:add, :many, data}} ->
        spawn(fn -> prepare(message_module, data_receiver, data) end)
        loop(message_module, data_receiver, id, secret)

      {:message, :kill} ->
        :ok

      any ->
        IO.puts("Unknown Message: PreparationModule")
        IO.puts(inspect(any))
        loop(message_module, data_receiver, id, secret)
    end
  end

  defp prepare(message_module, data_receiver, data) do
    prepared_data = Enum.map(data, fn line -> line_to_temp_data line end)
    |> Enum.filter(fn line -> line != :eof end)
    MessageModule.add_message(message_module, data_receiver, {:preparation_module, :data, prepared_data})
  end

  # Convert line to temp-data map
  defp line_to_temp_data line do
    case String.split(line, ";") do
      [station, date, _quality_level, temp, hum] ->
        date = String.trim(date)
        {year, _} = Integer.parse(String.slice(date, 0..3))
        {month, _} = Integer.parse(String.slice(date, 4..5))
        {day, _} = Integer.parse(String.slice(date, 6..7))
        {hour, _} = Integer.parse(String.slice(date, 8..9))
        {station, _} = Integer.parse(String.trim(station))
        {temp, _} = Float.parse(String.trim(temp))
        {hum, _} = Float.parse(String.trim(hum))
        %{
          "station" => station,
          "year" => year,
          "month" => month,
          "day" => day,
          "hour" => hour,
          "temp" => temp,
          "hum" => hum
        }
      [""] -> :eof
    end
  end

end
