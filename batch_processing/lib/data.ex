defmodule BatchProcessing.Data do

  defstruct db_connection: nil, map_reduce: nil, stations: [], actual_station: nil, results: %{}, views: %{}, receiver: []

end
