add 50ms buffer for after we submit our last cancellation till the time we can send off a new quote. ie, we can't make any trades for at least 50ms after we make a cancel. note, this should not be in addition to the 50ms clock on the quoting system, but should be taking it into account. eg if the quoting clock has 20ms on it when we send off a cancel, then we'll just add 30ms to it, to make it 50ms before our next quote check. 

are our latency metrics actually correct?

make the prices on the csv the adjusted prices not the raw price pushes

clean up the graph

clean up tech debt

fix up the logs that print out whenever I run it. there are no zero fills, or 0 latency things

figure out a way to fix hysteresis when spreads between exchanges blow out and my demeaning engine hasn't had time to catch up. maybe need to figure out a better solution that's not just de-meaning 

look through for anything that's not hft like, and report back on it. eg any sleeps, any latency stuff we shouldn't be doing, anything we're measuring wrong etc

separate switch for logging on and off, for max performance (can turn logger off from yaml file for max performance). 
