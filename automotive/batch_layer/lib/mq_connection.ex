defmodule BatchLayer.MqConnection do

  def start(mq, bl_data, id, secret) do
    MessageQueue.add_module(mq, id, secret)
    spawn(fn -> recv_messages(mq, bl_data, id, secret) end)
  end

  def send_results(mq, results) do
    MessageQueue.add_message(mq, :serving_layer, {:batch_layer, :result, results})
  end

  defp recv_messages(mq, bl_data, id, secret) do
    case MessageQueue.get_message(mq, id, secret) do
      {:message, :no_message} ->
        recv_messages(mq, bl_data, id, secret)

      {:message, {:add, :data, data}} ->
        IO.puts("Add Data")
        BatchLayer.add_data(bl_data, data)
        recv_messages(mq, bl_data, id, secret)

      {:message, :kill} ->
        IO.puts("Kill")
        BatchLayer.stop(bl_data)

      any ->
        IO.puts("Unknown Message: BatchLayer MQ Connection")
        IO.puts(inspect(any))
        recv_messages(mq, bl_data, id, secret)
    end
  end

end
