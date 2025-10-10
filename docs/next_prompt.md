add 50ms buffer for after we submit our last cancellation till the time we can send off a new quote. ie, we can't make any trades for at least 50ms after we make a cancel. note, this should not be in addition to the 50ms clock on the quoting system, but should be taking it into account. eg if the quoting clock has 20ms on it when we send off a cancel, then we'll just add 30ms to it, to make it 50ms before our next quote check. 

are our latency metrics actually correct?

make the prices on the csv the adjusted prices not the raw price pushes

clean up the graph

clean up tech debt

fix up the logs that print out whenever I run it. there are no zero fills, or 0 latency things

figure out a way to fix hysteresis when spreads between exchanges blow out and my demeaning engine hasn't had time to catch up. maybe need to figure out a better solution that's not just de-meaning 

look through for anything that's not hft like, and report back on it. eg any sleeps, any latency stuff we shouldn't be doing, anything we're measuring wrong etc

separate switch for logging on and off, for max performance (can turn logger off from yaml file for max performance). 

verify that im picking up all the data by running alongside my python mm data puller

maybe implement the bbo queue reduction stuff, where if I have multiple BBOs in the queue, just remove the older ones - only the newer ones matter, as feed is monotonic. see if that's already been done, and if not, do it maybe. 

we need to have the trades that we make (ie, private trades) use the exchange's timestamp, not our wire timestamp. actually, we should probably log both, and modify the graph to show both of them

need to get a better idea for what the quote start and quote ends represent. whether they are our wire time, or the exchange time. maybe I should measure both wire time and exchange time, so that I can get better visibility on how latent we are. there are a few data points which still look latent, but I'm not super sure which is which. need to understand whether our adverse selections are model based, or whether we can still latency minimise and get improvments still. maybe eventually, we can have an event driven taker order hot path for when things get really crazy. Will need to think heavily about model development though. 

improve the things that codex told me to. 

Recovery from feed/execution disconnects backs off with hard sleeps up to 30 s, which is disastrous if a venue wiggles; both the generic WS worker and Gate execution loop block rather than retry immediately (src/base_classes/ws.rs:236, src/execution/gate_ws.rs:360).

Global state relies on std::sync::Mutex that gets locked inside async tasks; holding a blocking mutex while running on Tokio executors risks scheduler stalls and adds contention on every user-trade/report update (src/base_classes/state.rs:3, src/execution/gate_ws.rs:280, src/bin/gate_runner.rs:382).

The execution worker parses every frame with serde_json::from_str into Value and stamps times with SystemTime::now, both relatively heavy system calls on the critical path; message rate spikes will magnify this overhead and SystemTime isn’t monotonic (src/execution/gate_ws.rs:403, src/base_classes/ws.rs:190, src/bin/gate_runner.rs:351).