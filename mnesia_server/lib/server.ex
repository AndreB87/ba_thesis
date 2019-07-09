defmodule MnesiaServer.Server do

  alias :mnesia, as: Mnesia

  @doc """
  Starts the Database server
  """
  def start do
    :ok = Mnesia.start()
    pid = spawn(fn -> main_loop() end)
    Process.register(pid, Application.get_env(:mnesia_server, :name))
    :ok
  end


  defp main_loop do
    receive do
      :init ->
        Mnesia.create_schema([node()])
        main_loop()

      # Creates a new table
      #
      # table: Name of the table
      # columns: List of all Columns
      # ram_only -> true - Value of the table is saved in RAM only - not persistent
      #          -> false - Value of the table is stored on disk
      # address: Address the answer is send to
      {:create_table, table, columns, ram_only, address} ->
        spawn fn -> create_table table, columns, ram_only, address end
        main_loop()

      # Creates an index on the given column for the given table
      #
      # table: Table, the Index shall created for
      # index: Column, on which the index shall be created
      # address: Address the answer is send to
      {:create_index, table, index, address} ->
        spawn fn -> create_index table, index, address end
        main_loop()

      # Adds the given column to the given table
      #
      # table: Table, the Column shall be added to
      # column: Column, which shall be added to the Table
      # address: Address the answer is send to
      {:add_column, table, column, address} ->
        spawn fn -> add_column table, column, address end
        main_loop()

      # Read a value from the given table with the given ID
      #
      # table: Table, the value is stored in
      # id: ID of the Element
      # address: Address the answer is send to
      {:read, table, id, address} ->
        spawn fn -> read table, id, address end
        main_loop()

      # Read a value from a table where the given index column has the given value
      #
      # table: Table, the values are stored in
      # column: Column Name of the index
      # value: Value of the searched item
      # address: Address the answer is send to
      {:index_read, table, column, value, address} ->
        spawn fn -> index_read table, column, value, address end
        main_loop()

      # Return all values matching the given tuple
      #
      # table: Table, the values are stored in
      # match_tuple: List for matching the searched Objects
      # address: Address the answer is send to
      {:match_object, table, match_list, address} ->
        spawn fn -> match_object table, match_list, address end
        main_loop()

      # Return all elements matched by the query values
      #
      # table: Table, the Index shall created for
      # table_indices: Tuple with all indices of the table
      # query_values: List with a tuple for all constraints
      # selected_columns: Columns which shall be returned
      # address: Address the answer is send to
      {:read_select, table, table_indices, query_values, selected_columns, address} ->
        spawn fn -> read_select table, table_indices, query_values, selected_columns, address end
        main_loop()

      # Inserts date into the given table
      #
      # table: Table, the data shall be inserted
      # data: Data which shall be inserted
      # address: Address the answer is send to
      {:write, table, data, address} ->
        spawn fn -> write table, data, address end
        main_loop()

      :kill ->
        IO.puts("Shutdown")
        Mnesia.stop()

      any ->
        IO.puts("Unrecognized Message: MnesiaServer")
        IO.puts(inspect(any))
        main_loop()
    end
  end


  # Creates a new table
  #
  # `table`: Name of the table
  # `columns`: List of all Columns
  # `ram_only` -> true - Value of the table is saved in RAM only - not persistent
  #            -> false - Value of the table is stored on disk
  # `address`: Address the answer is send to
  defp create_table(table, columns, true, address) do
    result = Mnesia.create_table(table, [attributes: columns])
    send address, {:mnesia, result}
  end

  defp create_table(table, columns, false, address) do
    result = Mnesia.create_table(table, [attributes: columns, disc_copies:  [node()]])
    send address, {:mnesia, result}
  end

  defp create_table(_table, _columns, _ram_only, address) do
    send address, {:mnesia, {:error, :wrong_argument}}
  end


  #Creates an index on the given `column` for the given `table`
  #
  # `table`: Table, the Index shall created for
  # `index`: Column, on which the index shall be created
  # `address`: Address the answer is send to
  defp create_index(table, index, address) do
    case Mnesia.add_table_index(table, index) do
      {:atomic, :ok} ->
        send address, {:mnesia, :ok}
      {:aborted, description} ->
        send address, {:mnesia, {:error, description}}
    end
  end


  defp add_column(_table, _column, address) do
    send address, {:mnesia, {:error, :not_implemented_yet}}
  end


  defp read(table, id, address) do
    case Mnesia.transaction(fn -> Mnesia.read({table, id}) end) do
      {:atomic, [element]} ->
        send address, {:mnesia, {:ok, element}}

      {:atomic, []} ->
        send address, {:mnesia, {:error, :not_found}}

      anything ->
        send address, {:mnesia, {:error, anything}}
    end
  end


  defp index_read(table, column, value, address) do
    case Mnesia.transaction(fn -> Mnesia.index_read(table, value, column) end) do
      {:atomic, []} ->
        send address, {:mnesia, {:error, :not_found}}

      {:atomic, elements} ->
        send address, {:mnesia, {:ok, elements}}

      {:aborted, description} ->
        send address, {:mnesia, {:error, description}}

      anything ->
        send address, {:mnesia, {:error, anything}}
    end
  end


  defp match_object(table, match_list, address) do
    match_tuple = List.to_tuple [table | match_list]
    case Mnesia.transaction(fn -> Mnesia.match_object(match_tuple) end) do
      {:atomic, []} ->
        send address, {:mnesia, {:error, :not_found}}

      {:atomic, elements} ->
        send address, {:mnesia, {:ok, elements}}

      {:aborted, description} ->
        send address, {:mnesia, {:error, description}}

      anything ->
        send address, {:mnesia, {:error, anything}}
    end
  end


  defp read_select(_table, _table_indices, _query_values, _selected_columns, address) do
    send address, {:mnesia, {:error, :not_implemented_yet}}
  end


  defp write(table, data, address) do
    data_tuple = List.to_tuple([table | data])
    case Mnesia.transaction(fn -> Mnesia.write(data_tuple) end) do
      {:atomic, :ok} ->
        send address, {:mnesia, :ok}

      {:aborted, description} ->
        send address, {:mnesia, {:error, description}}

      anything ->
        send address, {:mnesia, {:error, anything}}
    end
  end

end
