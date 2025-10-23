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
s
maybe implement the bbo queue reduction stuff, where if I have multiple BBOs in the queue, just remove the older ones - only the newer ones matter, as feed is monotonic. see if that's already been done, and if not, do it maybe. 

we need to have the trades that we make (ie, private trades) use the exchange's timestamp, not our wire timestamp. actually, we should probably log both, and modify the graph to show both of them

need to get a better idea for what the quote start and quote ends represent. whether they are our wire time, or the exchange time. maybe I should measure both wire time and exchange time, so that I can get better visibility on how latent we are. there are a few data points which still look latent, but I'm not super sure which is which. need to understand whether our adverse selections are model based, or whether we can still latency minimise and get improvments still. maybe eventually, we can have an event driven taker order hot path for when things get really crazy. Will need to think heavily about model development though. 

improve the things that codex told me to. 

Recovery from feed/execution disconnects backs off with hard sleeps up to 30 s, which is disastrous if a venue wiggles; both the generic WS worker and Gate execution loop block rather than retry immediately (src/base_classes/ws.rs:236, src/execution/gate_ws.rs:360).

Global state relies on std::sync::Mutex that gets locked inside async tasks; holding a blocking mutex while running on Tokio executors risks scheduler stalls and adds contention on every user-trade/report update (src/base_classes/state.rs:3, src/execution/gate_ws.rs:280, src/bin/gate_runner.rs:382).

The execution worker parses every frame with serde_json::from_str into Value and stamps times with SystemTime::now, both relatively heavy system calls on the critical path; message rate spikes will magnify this overhead and SystemTime isn’t monotonic (src/execution/gate_ws.rs:403, src/base_classes/ws.rs:190, src/bin/gate_runner.rs:351).

fix the inventory issues where it doesn't respect the max notional. start by init with the starting size via a get req, and then stream updates to that via ws. refresh every min or so. 

binance orderbook isn't being used. neither is gate. honestly, the books might actually just be worse, and more laggy than everything else, so kill the orderbook feeds maybe? 

Gate WS stream error: WebSocket protocol error: Connection reset without closing handshake
^ still get a bunch of these. fix

risk engine is still fucked

think about how I want trading to work, with regards to whether it will use the last trade if a series of trades is pushed. Will need to de-noise this, because the last trade in the sequence has a higher probability of being incorrect sometimes, though it might be a good conservative cancellation signal. 

do a test to see why im having high latency on half my quotes

do a test to see what the time difference is between sending in a take vs a cancellation. run it a few times, and see what the server says is the time diff between when i send, and when i receive a message back. Will probs need to do it a few times to create a histogram.

once I know what that difference is, I know what my own internal latency must be, in order to be competitive for speed (I think I am currently, which is good). But compare that to my own internal latency.

If need be, I can probs do an internal speed blitz at some point. have a generator generate random market data, in a random walk, and then measure how long it takes my system. or have a bunch of ob messages and shit, and optimise for how long it takes to process those ob messages. Lessgoo. 

Okay, big change. Now that I have same exchange monotonic data feeds, I maybe lower thrash by tracking each single venue, and it’s price updates. so instead of quoting around absolute prices, I can quote around deltas. idk, there is something to not comparing between exchanges like I’m currently doing, but somehow going okay, venue a has updated downward, lets see if that is the legit price, and kind of quoting around that, rather than just all exchanges in a pool, and quoting around thattt. 

So the two main things for me to do are a) to work on my no thrash algorithm, to get a single smooth fair price for gate, and b) work in between exchange latencies. They will be the two features with the highest ROI. These will have the biggest PnL impact. Probably also adding more exchanges will help too.t

My pricing algorithm:
* Currently, we're quoting around the most recent wire price. Except we make sure all the updates have the right seq order, and we also make sure we're not going backward in time between any of the feeds, using the matching engine timestamp. 

* One way we can adjust for between venue thrash is to implement an algorithm which measures the price diffs for a single exchange, and then applies that onto my venue. So this would work if it's just two venues right? Applying the diffs. But a) it still assumed that my venues price is correct, and not prone to little mini outliers etc. I won't always be super optimal to quote around my venue. But even discounting that, we couldn't just add every exchanges diff, because then we'd get a bunch of thrash, and it still doesn't feel right. So I guess the two problems we have to solve are 1) ensuring that we don't assume our venues price is correct all the time and 2) fixing the multi exchange diff problem (idk, maybe there's some weird multi dimentional graph structure we could make, but I have absolutely no clue how this would look whatsoever, and there's almost definitely an easier way to formulate this problem). We also need some way of weighting against the trustworthyness of each exchange - ie exchange x may lead price discovery at a given point in time more than another, and so we need to trust that venue more if it leads price discovery, so this may need to be some kind of a regression that we re-fit pretty frequently. Maybe another modification i can make is mean reversion stuff, where you get a spike, and within a timeframe of before you can send in new quotes to the exchange and have them be processed, price moves back downward - kind of like a deterministic level mean reversion, that maybe can be baked into the model. Like, some mean reversion threshold, where if you spike upward by 2% for eg, then you'll mean revert back down to 1.8% upward, and so you should quote around the 1.8%. And we also have two kinds of thrash - same exchange thrash, and between exchange thrash. Between exchange thrash seems to be way worse than single exchange thrash, so that will be the main problem to solve. Also, same exchange thrash is also an issue, but less of a priority, and also much more difficult, because it's hard to say that our venues price is fully incorrect at a given point in time. - Interesting point on this same venue thrash stuff, with when the top of book update is sent out, and the levels behind the top one arre super far away - so there's like, a perceptual gap in the orderbook / the top of book bid / ask is stale, is the normal way of phrasing that. 

* Next step - to measure lateness / lagginess between exchanges. Can think about it as follows: Sometimes an exchange will get overwhelmed with trades, or for whatever reason, will lag behind other exchanges. And so like, lets think about it in the context of a flash crash. You'll have two prices, you'll have the price that the venue is pushing out now, and then you'll have the price where if everybody stopped quoting right now, and only the orders in the internet cable + the exchanges order queue got processed, where prices would end up. And I guess the time diff between each of those. Which means that if the exchange is latent (more latent than it usually is, for a given update), that update is less likely to be representative price discovery. Which means that under perfect information, where a taker doesn't asymetrically skew on one exchange vs another, then if the exchange is more latent, then we can probably down weight it some amount. The thing I don't know how to comprehend is how this down weighting would work in less than perfect information, or say, when a latent exchange actually leads price discovery for the period that it's latent for (eg, what if they just get a cluster of trades, and those trades are price discovery, but we discount it because it's more laggy processing that trade cluster?)

* Have the volume weighted mid price. And then do a regression estimate of how much of that to apply to the mid. Several ways you can do it - turn it into a ratio and then regression estimate the next mid price on that ratio. Throw that, and the raw, unadjusted mid price into a regression, and figure out weights for each of them, and adjust on the fly (might be neat). 

