defmodule ServingLayer do

  @moduledoc """
  Serving Layer for the Lambda Architecture.
  Serves the Results from Batch and Speed Layer.
  """

  alias ServingLayer.Server, as: Server
  alias ServingLayer.MqConnection, as: MqConnection

  @doc """
  Initialises the Serving Layer and the MNesia Database.

  `ram_only` -> `true` -> Results are saved in RAM only (not persistent)
                `false` -> Results are saved on harddisk (persistent)
  """
  def init(ram_only \\false) do
     Server.init(ram_only)
  end

  @doc """
  Starts the ServingLayer ServingLayer.

  ## Examples
    iex> ServingLayer.start()
    # {ok, ServingLayerPID}
  """
  def start(mq) do
    {:ok, sl_pid} = Server.start()
    MqConnection.start(mq, sl_pid, :serving_layer, :serving_layer_secret)
    {:ok, sl_pid}
  end

  @doc """
  Stops the ServingLayer Process.
  """
  def stop(sl_pid) do
    send(sl_pid, :kill)
  end

  @doc """
  Returns the comlete average of temperature and humidity for all time and stations.
  """
  def get_result(sl_pid) do
    send(sl_pid, {:get, :result, :avg_complete, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `year`.
  """
  def get_result(sl_pid, year) do
    send(sl_pid, {:get, :result, {:year, year}, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `month` in `year`.
  """
  def get_result(sl_pid, year, month) do
    send(sl_pid, {:get, :result, {:month, year, month}, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `day` of the `month` in `year`.
  """
  def get_result(sl_pid, year, month, day) do
    send(sl_pid, {:get, :result, {:day, year, month, day}, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `station`.
  """
  def get_station_result(sl_pid, station) do
    send(sl_pid, {:get, :result, {:station, station}, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `station` in `year`.
  """
  def get_station_result(sl_pid, station, year) do
    send(sl_pid, {:get, :result, {:station, station, year}, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `station` in `month` of the `year`.
  """
  def get_station_result(sl_pid, station, year, month) do
    send(sl_pid, {:get, :result, {:station, station, year, month}, self()})
    receive_fun()
  end

  @doc """
  Returns the average temperature and humidity for the `station` on `day` of the `month` in `year`.
  """
  def get_station_result(sl_pid, station, year, month, day) do
    send(sl_pid, {:get, :result, {:station, station, year, month, day}, self()})
    receive_fun()
  end

  defp receive_fun do
    receive do
      any ->
        any
    end
  end

end
