defmodule ApplicationProvider.Analytics do
  @moduledoc """
  Documentation for BatchLayer.
  """

  alias FrameworkProvider.MessageModule, as: MessageModule

  @doc """
  Starts the BatchLayer.

  `message_queue` -> PID or Tuple with Name and Node-Name of Message Queue
  `batch_processing` -> PID or Tuple with Name and Node-Name of Batch Processing Module
  """
  def start(message_module, batch_processing, receiver) do
    receive_pid = spawn(fn -> receive_loop(message_module, receiver) end)
    BatchProcessing.add_receiver(batch_processing, receive_pid)
    :ok
  end

  @doc """
  Stops the Batch Layer

  `bl_data` -> BatchLayer Data
  """
  def stop(batch_processing) do
    BatchProcessing.stop(batch_processing)
  end

  defp send_results(message_module, receiver, results) do
    MessageModule.add_message(message_module, receiver, {:analytics, :result, results})
  end

  # Loop to receive Results from BatchProcessing Process
  #
  # serving_layer -> Serving Layer PID or Tuple
  defp receive_loop(message_module, receiver) do
    receive do
      {:batch_process, :result, results} ->
        IO.puts("Send Results to Message Module")
        send_results(message_module, receiver, results)
        receive_loop(message_module, receiver)

      :kill ->
        :ok

      any ->
        IO.puts("Unrecognized Message: Analytics")
        IO.puts(inspect(any))
        receive_loop(message_module, receiver)
    end
  end

end
