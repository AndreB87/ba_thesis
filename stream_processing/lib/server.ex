defmodule StreamProcessing.Server do

  alias StreamProcessing.Data, as: Data

  @doc """
  Starts the Stream Processing Process
  """
  def start() do
    pid = spawn(fn -> loop(%Data{}) end)
    Process.register(pid, Application.get_env(:stream_processing, :name))
    {:ok, pid}
  end

  # Main loop of the Speed Processing Process
  defp loop(sp_data) do
    receive do
      {:data, :add, temp_data} ->
        add_data(sp_data, temp_data)
        |> send_results
        |> loop

      {:data, :add_many, temp_data_list} ->
        add_many(sp_data, temp_data_list)
        |> send_results
        |> loop

      {:receiver, :add, receiver} ->
        add_receiver(sp_data, receiver)
        |> loop

      {:receiver, :remove, receiver} ->
        remove_receiver(sp_data, receiver)
        |> loop

      {:result, :remove, key} ->
        remove_from_result(sp_data, key)
        |> loop

      {:result, :reset} ->
        result_reset(sp_data)
        |> loop

      :kill ->
        IO.puts("Kill")
        :ok

      any ->
        IO.puts("Unknown Message: Stream Processing")
        IO.puts(inspect(any))
        loop(sp_data)
    end
  end

  # Adds Data to the Speed Processing Process
  defp add_data(sp_data, temp_data) do
    %Data{sp_data | results: compute_results(sp_data.results, temp_data)}
  end

  # Adds a List of Data to the Speed Processing Process
  defp add_many(sp_data, []) do
    sp_data
  end

  defp add_many(sp_data, [head | tail]) do
    add_many(add_data(sp_data, head), tail)
  end

  # Adds a Receiver to the Speed Processing Process
  defp add_receiver(sp_data, receiver) do
    %Data{sp_data | receiver: sp_data.receiver ++ [receiver]}
  end

  # Removes a Receiver from the Speed Processing Process
  defp remove_receiver(sp_data, receiver) do
    %Data{sp_data | receiver: sp_data.receiver -- [receiver]}
  end

  # Remove a Result from the result map
  defp remove_from_result(sp_data, key) do
    %Data{sp_data | results: Map.delete(sp_data.results, key)}
  end

  # Reset the Result Map
  defp result_reset(sp_data) do
    %Data{sp_data | results: %{}}
  end

  # Compute new results with a neew data set
  defp compute_results(results, temp_data) do
    compute_results(results, temp_data, get_keys())
  end

  defp compute_results(results, _temp_data, []) do
    results
  end

  defp compute_results(results, temp_data, [key | tail]) do
    if key == :avg_complete do
      result = Map.get(results, key, nil)
      Map.put(results, key, compute_avg(result, temp_data))
      |> compute_results(temp_data, tail)
    else
      sub_key = get_sub_key(key, temp_data)
      sub_results = Map.get(results, key, %{})
      result = Map.get(sub_results, sub_key, nil)
      Map.put(results, key, Map.put(sub_results, sub_key, compute_avg(result, temp_data)))
      |> compute_results(temp_data, tail)
    end
  end

  # Compute new average with a neew data set
  defp compute_avg(nil, %{"temp" => temp, "hum" => hum}) do
    {temp_elements, avg_temp} = compute_avg_val(0, 0, temp)
    {hum_elements, avg_hum} = compute_avg_val(0, 0, hum)
    %{
      elements: 1,
      temp_elements: temp_elements,
      avg_temp: avg_temp,
      hum_elements: hum_elements,
      avg_hum: avg_hum
    }
  end

  defp compute_avg(result, %{"temp" => temp, "hum" => hum}) do
    elements = result.elements + 1
    {temp_elements, avg_temp} = compute_avg_val(result.temp_elements, result.avg_temp, temp)
    {hum_elements, avg_hum} = compute_avg_val(result.hum_elements, result.avg_hum, hum)
    %{
      elements: elements,
      temp_elements: temp_elements,
      avg_temp: avg_temp,
      hum_elements: hum_elements,
      avg_hum: avg_hum
    }
  end

  # Compute average with old result and value of new data set`
  defp compute_avg_val(old_count, old_avg, new_val) do
    if new_val > -999 do
      new_count = old_count + 1
      {new_count, ((old_avg * old_count) + new_val) / new_count}
    else
      {old_count, old_avg}
    end
  end

  defp get_keys do
     [
       :avg_complete,
       :avg_year,
       :avg_month,
       :avg_day,
       :avg_station_complete,
       :avg_station_year,
       :avg_station_month,
       :avg_station_day
     ]
  end

  defp send_results(sp_data) do
    Enum.each(sp_data.receiver, fn receiver -> send(receiver, {:stream_processing, :result, sp_data.results}) end)
    sp_data
  end

  defp get_sub_key(key, temp_data) do
    %{
      "station" => station,
      "year" => year,
      "month" => month,
      "day" => day
    } = temp_data
    case key do
      :avg_year ->
        year
      :avg_month ->
        {year, month}
      :avg_day ->
        {year, month, day}
      :avg_station_complete ->
        station
      :avg_station_year ->
        {station, year}
      :avg_station_month ->
        {station, year, month}
      :avg_station_day ->
        {station, year, month, day}
      _any ->
        :error
    end
  end

end
