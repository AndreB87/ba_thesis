defmodule SpeedLayer.MqConnection do

  def start(mq, sl_data, id, secret) do
    MessageQueue.add_module(mq, id, secret)
    spawn(fn -> recv_messages(mq, sl_data, id, secret) end)
  end

  def send_to_batch_layer(mq, data) do
    MessageQueue.add_message(mq, :batch_layer, {:add, :data, data})
  end

  def send_results(mq, results) do
    MessageQueue.add_message(mq, :serving_layer, {:speed_layer, :result, results})
  end

  defp recv_messages(mq, sl_data, id, secret) do
    case MessageQueue.get_message(mq, id, secret) do
      {:message, :no_message} ->
        recv_messages(mq, sl_data, id, secret)

      {:message, {:add, :many, data}} ->
        IO.puts("Add Data")
        SpeedLayer.add_many(sl_data, mq, data)
        recv_messages(mq, sl_data, id, secret)

      {:message, :kill} ->
        IO.puts("Kill")
        SpeedLayer.stop(sl_data)

      any ->
        IO.puts("Unknown Message: SpeedLayer MQ Connection")
        IO.puts(inspect(any))
        recv_messages(mq, sl_data, id, secret)
    end
  end

end
