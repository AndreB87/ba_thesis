defmodule BatchProcessing.Server do
  @moduledoc """
  Process for BatchProcessing of the temp-data
  """

  alias BatchProcessing.Cassandra, as: Cassandra
  alias BatchProcessing.Data, as: Data

  @doc """
  Start the BatchProcessing Server
  """
  def start(db_pid, mr_pid) do
    data = add_views(%Data{db_connection: db_pid, map_reduce: mr_pid})
    pid = spawn(fn -> start_calculation(data) end)
    Process.register(pid, Application.get_env(:batch_processing, :name))
    register_map_reduce(data.map_reduce, pid)
    {:ok, pid}
  end

  defp start_calculation(data) do
    {:ok, stations} = Cassandra.get_unique_values(data.db_connection, :station)
    station_list = Enum.map(stations, fn station -> Map.fetch!(station, "station")  end)
    %Data{data | stations: station_list, actual_station: nil, results: %{}}
    |> calculate_station
    |> loop
  end

  # Register BatchProcessing.Server at MapReduce
  defp register_map_reduce(map_reduce, pid) do
    MapReduce.register(map_reduce, pid)
  end

  defp calculate_station(data) do
    case data.stations do
      [station | rest] ->
        {:ok, temp_list} = Cassandra.get_temp_data_station(data.db_connection, station)
        calculate_data(data.map_reduce, temp_list)
        %Data{data | stations: rest, actual_station: station}

      [] ->
        data
    end
  end

  defp recalculate_station(data) do
    {:ok, temp_list} = Cassandra.get_temp_data_station(data.db_connection, data.actual_station)
     calculate_data(data.map_reduce, temp_list)
  end

  # Send data for calculation to the map reduce process
  defp calculate_data(map_reduce, temp_list) do
    send map_reduce, {:calculation, :start, temp_list, self()}
  end

  # Main loop
  defp loop(data) do
    receive do
      {:data, :add, temp_data} ->
        add_data(data.db_connection, temp_data)
        loop(data)

      {:data, :add_many, temp_list} ->
        add_many(data.db_connection, temp_list)
        loop(data)

      {:data, :correct, temp_data} ->
        correct_data(data.db_connection, temp_data)
        loop(data)

      {:data, :delete, station, year, month, day, hour} ->
        delete_data(data.db_connection, station, year, month, day, hour)
        loop(data)

      {:data, :get, station, year, month, day, hour, pid} ->
        get_data(data.db_connection, station, year, month, day, hour, pid)
        loop(data)

      {:node, :add, type, key, function} ->
        add_node(data.map_reduce, type, key, function)
        loop(data)

      {:receiver, :add, pid} ->
        add_receiver(data, pid)
        |> loop

      {:map_reduce, :result, result} ->
        add_results(data, result)

      {:map_reduce, :error, :in_progress} ->
        recalculate_station(data)
        loop(data)

      {:map_reduce, :error, :no_node} ->
        recalculate_station(data)
        loop(data)

      :kill ->
        IO.puts("Kill")
        MapReduce.stop(data.map_reduce)

      any ->
        IO.puts("Unrecognized Message: BatchProcessing")
        IO.puts inspect any
        loop data
    end
  end

  # Adds temp_data to the Database
  defp add_data(db_connection, temp_data) do
     :ok = Cassandra.insert_temp_data(db_connection, temp_data)
  end

  # Adds a list of temp_data to the Database
  defp add_many(db_connection, temp_list) do
    Enum.each(temp_list, fn temp_data -> add_data(db_connection, temp_data) end)
  end

  # Correct the data_d
  defp correct_data(db_connection, data) do
    :ok = Cassandra.correct_temp_data(db_connection, data)
  end

  # Deletes the data
  defp delete_data(db_connection, station, year, month, day, hour) do
    :ok = Cassandra.delete_temp_data(db_connection, station, year, month, day, hour)
  end

  defp get_data(db_connection, station, year, month, day, hour, pid) do
    {:ok, temp_data} = Cassandra.get_temp_data_station(db_connection, station, year, month, day, hour)
    send(pid, {:batch_processing, :data, temp_data})
  end

  # Adds a node to the map reduce process
  defp add_node(map_reduce, type, key, function) do
    {:ok, _pid} = MapReduce.add_node(map_reduce, type, key, function)
  end

  # Adds a receiver to batch processing application
  defp add_receiver(data, pid) do
    %Data{data | receiver: [pid | data.receiver]}
  end

  # Adds result from map reduce process to the results
  defp add_results(data, results) do
    add_results(data, results, Map.keys(results))
    |> check_finished()
  end

  defp add_results(data, _results, []) do
    data
  end

  defp add_results(data, results, [key | tail]) do
    {_map, reduce} = Map.fetch!(data.views, key)
    case Map.fetch(data.results, key) do
      :error ->
        %Data{data | results: Map.put(data.results, key, Map.fetch!(results, key))}
        |> add_results(results, tail)

      {:ok, old_res} ->
        case Map.fetch(results, key) do
          :error ->
            add_results(data, results, tail)
          {:ok, new_res} ->
            data = %Data{data | results: Map.put(data.results, key, reduce.([old_res, new_res]))}
            add_results(data, results, tail)
        end
    end
  end

  # Check if calculation is finished
  defp check_finished(data) do
    case data.stations do
      [] ->
        send_result(data)

      _any ->
        calculate_station(data)
        |> loop
    end
  end

  # Send result to listeners
  defp send_result(data) do
    Enum.each(data.receiver, fn receiver -> send receiver, {:batch_process, :result, data.results} end)
    start_calculation(data)
  end

  # Add views to MapReduce Server
  defp add_views(data) do
    views = %{
      avg_complete: {&avg_complete_map/1, &avg_complete_reduce/1},
      avg_year: {&avg_year_map/1, &avg_year_reduce/1},
      avg_month: {&avg_month_map/1, &avg_month_reduce/1},
      avg_day: {&avg_day_map/1, &avg_day_reduce/1},
      avg_station_complete: {&avg_station_complete_map/1, &avg_station_complete_reduce/1},
      avg_station_year: {&avg_station_year_map/1, &avg_station_year_reduce/1},
      avg_station_month: {&avg_station_month_map/1, &avg_station_month_reduce/1},
      avg_station_day: {&avg_station_day_map/1, &avg_station_day_reduce/1}
    }

    Enum.each(
      views,
      fn {key, {map, reduce}} ->
        Enum.each(0..3, fn _x -> MapReduce.add_node(data.map_reduce, :map, key, map) end);
        MapReduce.add_node(data.map_reduce, :reduce, key, reduce)
      end)
    %Data{data | views: views}
  end

  ##########################################
  ### Map - Reduce functions
  ##########################################

  # Complete average

  # Calculates complete average
  defp avg_complete_map(temp_data_list) do
    elements = length(temp_data_list);
    temp_list = Enum.filter(Enum.map(temp_data_list, fn line -> Map.fetch!(line, "temp") end), fn data -> data > -900 end);
    temp_elements = length(temp_list);
    hum_list = Enum.filter(Enum.map(temp_data_list, fn line -> Map.fetch!(line, "hum") end), fn data -> data > -900 end);
    hum_elements = length(hum_list);
    %{
      elements: elements,
      avg_temp: compute_avg(temp_list),
      temp_elements: temp_elements,
      avg_hum: compute_avg(hum_list),
      hum_elements: hum_elements
    }
  end

  defp avg_complete_reduce([accu]) do
     accu
  end

  defp avg_complete_reduce([first, second | tail]) do
    elements = first.elements + second.elements;
    temp_elements = first.temp_elements + second.temp_elements;
    hum_elements = first.hum_elements + second.hum_elements;
    accu = %{
      elements: elements,
      avg_temp: avg_reduce(first.avg_temp, first.temp_elements, second.avg_temp, second.temp_elements),
      temp_elements: temp_elements,
      avg_hum: avg_reduce(first.avg_hum, first.hum_elements, second.avg_hum, second.hum_elements),
      hum_elements: hum_elements
    }
    avg_complete_reduce([accu | tail])
  end

  # Average by year

  # Calculates the average of years
  defp avg_year_map(data) do
    # Split data into years
    data_y = Enum.group_by(data, fn line -> Map.fetch!(line, "year") end);
    # Calculate average of years
    avg_y = Enum.map(
      Map.keys(data_y),
      fn year ->
        elements = length(Map.fetch!(data_y, year));
        temp_list = Enum.filter(Enum.map(Map.fetch!(data_y, year), fn line -> Map.fetch!(line, "temp") end), fn data -> data > -900 end);
        temp_elements = length(temp_list);
        hum_list = Enum.filter(Enum.map(Map.fetch!(data_y, year), fn line -> Map.fetch!(line, "hum") end), fn data -> data > -900 end);
        hum_elements = length(hum_list);
        %{
          year: year,
          elements: elements,
          avg_temp: compute_avg(temp_list),
          temp_elements: temp_elements,
          avg_hum: compute_avg(hum_list),
          hum_elements: hum_elements
        }
      end
    );
    # Group averages by year for easier reduction
    Enum.group_by(avg_y, fn line -> line.year end)
  end

  # Reduces the average of years
  defp avg_year_reduce([accu]) do
     accu
  end

  defp avg_year_reduce([first, second | tail]) do
    # Reduce average of years
    accu = Map.merge(
      first,
      second,
      fn year, [first_v], [second_v] ->
        elements = first_v.elements + second_v.elements;
        temp_elements = first_v.temp_elements + second_v.temp_elements;
        hum_elements = first_v.hum_elements + second_v.hum_elements;
        [%{
          year: year,
          elements: elements,
          avg_temp: avg_reduce(first_v.avg_temp, first_v.temp_elements, second_v.avg_temp, second_v.temp_elements),
          temp_elements: temp_elements,
          avg_hum: avg_reduce(first_v.avg_hum, first_v.hum_elements, second_v.avg_hum, second_v.hum_elements),
          hum_elements: hum_elements
        }]
      end
    )
    avg_year_reduce([accu | tail])
  end

  # Average by month

  # Calculates the average of months
  defp avg_month_map(data) do
    # Split date into years and months
    data_m = Enum.group_by(data, fn line -> {Map.fetch!(line, "year"), Map.fetch!(line, "month")} end);
    # Calculate average of months
    avg_m = Enum.map(
      Map.keys(data_m),
      fn m = {year, month} ->
        elements = length(Map.fetch!(data_m, m));
        temp_list = Enum.filter(Enum.map(Map.fetch!(data_m, m), fn line -> Map.fetch!(line, "temp") end), fn data -> data > -900 end);
        temp_elements = length(temp_list);
        hum_list = Enum.filter(Enum.map(Map.fetch!(data_m, m), fn line -> Map.fetch!(line, "hum") end), fn data -> data > -900 end);
        hum_elements = length(hum_list);
        %{
          year: year,
          month: month,
          elements: elements,
          avg_temp: compute_avg(temp_list),
          temp_elements: temp_elements,
          avg_hum: compute_avg(hum_list),
          hum_elements: hum_elements
        }
      end
    );
    # Group averages by year and month for easier reduction
    Enum.group_by(avg_m, fn line -> {line.year, line.month} end)
  end

  # Reduces the average of months
  defp avg_month_reduce([accu]) do
     accu
  end

  defp avg_month_reduce([first, second | tail]) do
    # Reduce average of months
    accu = Map.merge(
      first,
      second,
      fn {year, month}, [first_v], [second_v] ->
        elements = first_v.elements + second_v.elements;
        temp_elements = first_v.temp_elements + second_v.temp_elements;
        hum_elements = first_v.hum_elements + second_v.hum_elements;
        [%{
          year: year,
          month: month,
          elements: elements,
          avg_temp: avg_reduce(first_v.avg_temp, first_v.temp_elements, second_v.avg_temp, second_v.temp_elements),
          temp_elements: temp_elements,
          avg_hum: avg_reduce(first_v.avg_hum, first_v.hum_elements, second_v.avg_hum, second_v.hum_elements),
          hum_elements: hum_elements
        }]
      end
    )
    avg_month_reduce([accu | tail])
  end

  # Average by day

  # Calculates the average of days
  defp avg_day_map(data) do
    # Split date into years and months
    data_d = Enum.group_by(data, fn line -> {Map.fetch!(line, "year"), Map.fetch!(line, "month"), Map.fetch!(line, "day")} end);
    # Calculate average of months
    avg_d = Enum.map(
      Map.keys(data_d),
      fn d = {year, month, day} ->
        elements = length(Map.fetch!(data_d, d));
        temp_list = Enum.filter(Enum.map(Map.fetch!(data_d, d), fn line -> Map.fetch!(line, "temp") end), fn data -> data > -900 end);
        temp_elements = length(temp_list);
        hum_list = Enum.filter(Enum.map(Map.fetch!(data_d, d), fn line -> Map.fetch!(line, "hum") end), fn data -> data > -900 end);
        hum_elements = length(hum_list);
        %{
          year: year,
          month: month,
          day: day,
          elements: elements,
          avg_temp: compute_avg(temp_list),
          temp_elements: temp_elements,
          avg_hum: compute_avg(hum_list),
          hum_elements: hum_elements
        }
      end
    );
    # Group averages by year, month and day for easier reduction
    Enum.group_by(avg_d, fn line -> {line.year, line.month, line.day} end)
  end

  defp avg_day_reduce([accu]) do
     accu
  end

  # Reduces the average of days
  defp avg_day_reduce([first, second | tail]) do
    # Reduce average of days
    accu = Map.merge(
      first,
      second,
      fn {year, month, day}, [first_v], [second_v] ->
        elements = first_v.elements + second_v.elements;
        temp_elements = first_v.temp_elements + second_v.temp_elements;
        hum_elements = first_v.hum_elements + second_v.hum_elements;
        [%{
          year: year,
          month: month,
          day: day,
          elements: elements,
          avg_temp: avg_reduce(first_v.avg_temp, first_v.temp_elements, second_v.avg_temp, second_v.temp_elements),
          temp_elements: temp_elements,
          avg_hum: avg_reduce(first_v.avg_hum, first_v.hum_elements, second_v.avg_hum, second_v.hum_elements),
          hum_elements: hum_elements
        }]
      end
    )
    avg_day_reduce([accu | tail])
  end

  # Complete average of stations

  # Calculates complete average of stations
  defp avg_station_complete_map(data) do
    # Split data into years
    data_s = Enum.group_by(data, fn line -> Map.fetch!(line, "station") end);
    # Calculate average of years
    avg_s = Enum.map(
      Map.keys(data_s),
      fn station ->
        elements = length(Map.fetch!(data_s, station));
        temp_list = Enum.filter(Enum.map(Map.fetch!(data_s, station), fn line -> Map.fetch!(line, "temp") end), fn data -> data > -900 end);
        temp_elements = length(temp_list);
        hum_list = Enum.filter(Enum.map(Map.fetch!(data_s, station), fn line -> Map.fetch!(line, "hum") end), fn data -> data > -900 end);
        hum_elements = length(hum_list);
        %{
          station: station,
          elements: elements,
          avg_temp: compute_avg(temp_list),
          temp_elements: temp_elements,
          avg_hum: compute_avg(hum_list),
          hum_elements: hum_elements
        }
      end
    );
    # Group averages by year for easier reduction
    Enum.group_by(avg_s, fn line -> line.station end)
  end

  # Reduces the complete average of stations
  defp avg_station_complete_reduce([accu]) do
     accu
  end

  defp avg_station_complete_reduce([first, second | tail]) do
    # Reduce average of years
    accu = Map.merge(
      first,
      second,
      fn station, [first_v], [second_v] ->
        elements = first_v.elements + second_v.elements;
        temp_elements = first_v.temp_elements + second_v.temp_elements;
        hum_elements = first_v.hum_elements + second_v.hum_elements;
        [%{
          station: station,
          elements: elements,
          avg_temp: avg_reduce(first_v.avg_temp, first_v.temp_elements, second_v.avg_temp, second_v.temp_elements),
          temp_elements: temp_elements,
          avg_hum: avg_reduce(first_v.avg_hum, first_v.hum_elements, second_v.avg_hum, second_v.hum_elements),
          hum_elements: hum_elements
        }]
      end
    )
    avg_station_complete_reduce([accu | tail])
  end

  # Average by year of stations

  # Calculates the average of years of stations
  defp avg_station_year_map(data) do
    # Split data into years
    data_y = Enum.group_by(data, fn line -> {Map.fetch!(line, "station"), Map.fetch!(line, "year")} end);
    # Calculate average of years
    avg_y = Enum.map(
      Map.keys(data_y),
      fn y = {station, year} ->
        elements = length(Map.fetch!(data_y, y));
        temp_list = Enum.filter(Enum.map(Map.fetch!(data_y, y), fn line -> Map.fetch!(line, "temp") end), fn data -> data > -900 end);
        temp_elements = length(temp_list);
        hum_list = Enum.filter(Enum.map(Map.fetch!(data_y, y), fn line -> Map.fetch!(line, "hum") end), fn data -> data > -900 end);
        hum_elements = length(hum_list);
        %{
          station: station,
          year: year,
          elements: elements,
          avg_temp: compute_avg(temp_list),
          temp_elements: temp_elements,
          avg_hum: compute_avg(hum_list),
          hum_elements: hum_elements
        }
      end
    );
    # Group averages by year for easier reduction
    Enum.group_by(avg_y, fn line -> {line.station, line.year} end)
  end

  # Reduces the average of years of stations
  defp avg_station_year_reduce([accu]) do
     accu
  end

  defp avg_station_year_reduce([first, second | tail]) do
    # Reduce average of years
    accu = Map.merge(
      first,
      second,
      fn {station, year}, [first_v], [second_v] ->
        elements = first_v.elements + second_v.elements;
        temp_elements = first_v.temp_elements + second_v.temp_elements;
        hum_elements = first_v.hum_elements + second_v.hum_elements;
        [%{
          station: station,
          year: year,
          elements: elements,
          avg_temp: avg_reduce(first_v.avg_temp, first_v.temp_elements, second_v.avg_temp, second_v.temp_elements),
          temp_elements: temp_elements,
          avg_hum: avg_reduce(first_v.avg_hum, first_v.hum_elements, second_v.avg_hum, second_v.hum_elements),
          hum_elements: hum_elements
        }]
      end
    )
    avg_station_year_reduce([accu | tail])
  end

  # Average by month of stations

  # Calculates the average of months of stations
  defp avg_station_month_map(data) do
    # Split date into years and months
    data_m = Enum.group_by(data, fn line -> {Map.fetch!(line, "station"), Map.fetch!(line, "year"), Map.fetch!(line, "month")} end);
    # Calculate average of months
    avg_m = Enum.map(
      Map.keys(data_m),
      fn m = {station, year, month} ->
        elements = length(Map.fetch!(data_m, m));
        temp_list = Enum.filter(Enum.map(Map.fetch!(data_m, m), fn line -> Map.fetch!(line, "temp") end), fn data -> data > -900 end);
        temp_elements = length(temp_list);
        hum_list = Enum.filter(Enum.map(Map.fetch!(data_m, m), fn line -> Map.fetch!(line, "hum") end), fn data -> data > -900 end);
        hum_elements = length(hum_list);
        %{
          station: station,
          year: year,
          month: month,
          elements: elements,
          avg_temp: compute_avg(temp_list),
          temp_elements: temp_elements,
          avg_hum: compute_avg(hum_list),
          hum_elements: hum_elements
        }
      end
    );
    # Group averages by year and month for easier reduction
    Enum.group_by(avg_m, fn line -> {line.station, line.year, line.month} end)
  end

  # Reduces the average of months of stations
  defp avg_station_month_reduce([accu]) do
     accu
  end

  defp avg_station_month_reduce([first, second | tail]) do
    # Reduce average of months
    accu = Map.merge(
      first,
      second,
      fn {station, year, month}, [first_v], [second_v] ->
        elements = first_v.elements + second_v.elements;
        temp_elements = first_v.temp_elements + second_v.temp_elements;
        hum_elements = first_v.hum_elements + second_v.hum_elements;
        [%{
          station: station,
          year: year,
          month: month,
          elements: elements,
          avg_temp: avg_reduce(first_v.avg_temp, first_v.temp_elements, second_v.avg_temp, second_v.temp_elements),
          temp_elements: temp_elements,
          avg_hum: avg_reduce(first_v.avg_hum, first_v.hum_elements, second_v.avg_hum, second_v.hum_elements),
          hum_elements: hum_elements
        }]
      end
    )
    avg_station_month_reduce([accu | tail])
  end

  # Average by day of stations

  # Calculates the average of days of stations
  defp avg_station_day_map(data) do
    # Split date into years and months
    data_d = Enum.group_by(data, fn line -> {Map.fetch!(line, "station"), Map.fetch!(line, "year"), Map.fetch!(line, "month"), Map.fetch!(line, "day")} end);
    # Calculate average of months
    avg_d = Enum.map(
      Map.keys(data_d),
      fn d = {station, year, month, day} ->
        elements = length(Map.fetch!(data_d, d));
        temp_list = Enum.filter(Enum.map(Map.fetch!(data_d, d), fn line -> Map.fetch!(line, "temp") end), fn data -> data > -900 end);
        temp_elements = length(temp_list);
        hum_list = Enum.filter(Enum.map(Map.fetch!(data_d, d), fn line -> Map.fetch!(line, "hum") end), fn data -> data > -900 end);
        hum_elements = length(hum_list);
        %{
          station: station,
          year: year,
          month: month,
          day: day,
          elements: elements,
          avg_temp: compute_avg(temp_list),
          temp_elements: temp_elements,
          avg_hum: compute_avg(hum_list),
          hum_elements: hum_elements
        }
      end
    );
    # Group averages by year, month and day for easier reduction
    Enum.group_by(avg_d, fn line -> {line.station, line.year, line.month, line.day} end)
  end

  # Reduces the average of days of stations
  defp avg_station_day_reduce([accu]) do
     accu
  end

  defp avg_station_day_reduce([first, second | tail]) do
    # Reduce average of days
    accu = Map.merge(
      first,
      second,
      fn {station, year, month, day}, [first_v], [second_v] ->
        elements = first_v.elements + second_v.elements;
        temp_elements = first_v.temp_elements + second_v.temp_elements;
        hum_elements = first_v.hum_elements + second_v.hum_elements;
        [%{
          station: station,
          year: year,
          month: month,
          day: day,
          elements: elements,
          avg_temp: avg_reduce(first_v.avg_temp, first_v.temp_elements, second_v.avg_temp, second_v.temp_elements),
          temp_elements: temp_elements,
          avg_hum: avg_reduce(first_v.avg_hum, first_v.hum_elements, second_v.avg_hum, second_v.hum_elements),
          hum_elements: hum_elements
        }]
      end
    )
    avg_station_day_reduce([accu | tail])
  end

  defp compute_avg([]) do
    0
  end

  defp compute_avg(list) do
     Enum.sum(list) / length(list)
  end

  defp avg_reduce(first_val, first_elements, second_val, second_elements) do
    all_elements = first_elements + second_elements
    if all_elements > 0 do
       ((first_val * first_elements) + (second_val * second_elements)) / all_elements
    else
      0
    end
  end

end
