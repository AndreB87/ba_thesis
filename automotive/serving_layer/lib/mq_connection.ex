defmodule ServingLayer.MqConnection do

  def start(mq, sl, id, secret) do
    MessageQueue.add_module(mq, id, secret)
    spawn(fn -> recv_messages(mq, sl, id, secret) end)
  end

  defp recv_messages(mq, sl, id, secret) do
    case MessageQueue.get_message(mq, id, secret) do
      {:message, :no_message} ->
        recv_messages(mq, sl, id, secret)

      {:message, {:speed_layer, :result, result}} ->
        IO.puts("Add Result")
        send(sl, {:speed_layer, :result, result})
        recv_messages(mq, sl, id, secret)

      {:message, {:batch_layer, :result, result}} ->
        IO.puts("Add Result")
        send(sl, {:batch_layer, :result, result})
        recv_messages(mq, sl, id, secret)

      {:message, :kill} ->
        IO.puts("Kill")
        ServingLayer.stop(sl)

      any ->
        IO.puts("Unknown Message: SpeedLayer MQ Connection")
        IO.puts(inspect(any))
        recv_messages(mq, sl, id, secret)
    end
  end

end
