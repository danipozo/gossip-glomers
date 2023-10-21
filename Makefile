bins:
	cargo build

echo: bins
	maelstrom/maelstrom test -w echo --bin target/debug/echo --node-count 1 --time-limit 10 --log-stderr

unique-ids: bins
	maelstrom/maelstrom test -w unique-ids --bin target/debug/unique_ids --node-count 3 --time-limit 30 --availability total --nemesis partition --log-stderr
