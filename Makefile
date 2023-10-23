bins:
	cargo build

echo: bins
	maelstrom/maelstrom test -w echo --bin target/debug/echo --node-count 1 --time-limit 10 --log-stderr

unique-ids: bins
	maelstrom/maelstrom test -w unique-ids --bin target/debug/unique_ids --node-count 3 --time-limit 30 --rate 1000 --availability total --nemesis partition --log-stderr

broadcast: bins
	maelstrom/maelstrom test -w broadcast --bin target/debug/broadcast --node-count 1 --time-limit 20 --rate 10

broadcast-multi: bins
	maelstrom/maelstrom test -w broadcast --bin target/debug/broadcast-multi --node-count 5 --time-limit 20 --rate 10

broadcast-fault-tolerant: bins
	maelstrom/maelstrom test -w broadcast --bin target/debug/broadcast-fault-tolerant --node-count 5 --time-limit 20 --rate 10 --nemesis partition
