path=$(dirname $0)
cd $path

elixir --sname mnesia -S mix run -e "MnesiaServer.start()" --no-halt &
elixir --sname batch -S mix run -e "BatchProcessing.start()" --no-halt &
elixir --sname stream -S mix run -e "StreamProcessing.start()" --no-halt &
iex --sname lambda -S mix 
