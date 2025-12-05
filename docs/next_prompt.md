

maybe implement the bbo queue reduction stuff, where if I have multiple BBOs in the queue, just remove the older ones - only the newer ones matter, as feed is monotonic. see if that's already been done, and if not, do it maybe. 

we need to have the trades that we make (ie, private trades) use the exchange's timestamp, not our wire timestamp. actually, we should probably log both, and modify the graph to show both of them

need to get a better idea for what the quote start and quote ends represent. whether they are our wire time, or the exchange time. maybe I should measure both wire time and exchange time, so that I can get better visibility on how latent we are. there are a few data points which still look latent, but I'm not super sure which is which. need to understand whether our adverse selections are model based, or whether we can still latency minimise and get improvments still. maybe eventually, we can have an event driven taker order hot path for when things get really crazy. Will need to think heavily about model development though. 

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

* I need to create a way of predicting the gate price with data from other exchanges. What that means is that I guess I need to use the gate price as some like fair value price. And then essentially, when I don't have data from gate, impute the price on gate with a confidence interval using data from other exchanges. The problem with this, though, is that it assumes that the gate price is always correct, which it might not be. And so I'll need to think about it because the way that you could make this work is that you have some kind of regression and then you figure out the fair price on gate, or you use the most recent price on gate instead. And you assume that that is the correct price, but it might not be. And so the thing that I'm unsure about, well, the thing that I don't want to do actually is to always use the gate price as the representative price because gate might have noise, and I don't want to be that affected by that noise. So I need an algorithm that doesn't 100% of the time respect Gate's price. It should almost all of the time because almost all of the time Gate will be correct, but it should not have the Gate price as the 100% fair price all of the time because it might be wrong 

## TODO 
* WE HAVE A BIG LATENCY ISSUE. But we've also started adding MEXC. if we can't fix the latency issue, we'll have to re-implement MEXC as a venue and git roll back. p90 latency is 1ms, which is awful. try to fix it first, and if that doesn't work, then roll back to a previous push.
* Fix trade size stuff for gate and okx
* Fix the weird gate order submission delay stuff, where some are at 10ms and others are at 20
* Make sure data structures are nice and fast. ob is optimised.
* Add in remote markouts (gate mid / bbo at various durations) that I can query from db / have work with grafana / something else thats good. Maybe can hook up some vercel type website instance so it's easy view?
* Add a bunch of exchanges as data sources (MEXC, hyperliquid, others)
* Make multi symbol compatable?
* Prepare codebase to have pricing algo so that things don't break


turn off naggle to stop kernel message coalessing

make sure sizing is working properly for gate and okx
