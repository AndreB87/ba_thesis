defmodule MapReduce.Data do
    @moduledoc """
    This Module holds all the Data the Batchlayer needs to compute new Batch-Views
    """
    defstruct in_progress: false, line: 0, map_nodes: %{}, used_map_nodes: %{}, reduce_nodes: %{}, receiver: [], map_results: %{}, reduce_results: %{}

end
