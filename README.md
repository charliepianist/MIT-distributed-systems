# MIT 6.824 - Distributed Systems

This is a repo of my solutions to the labs in MIT 6.824 Distributed Systems, which I self-studied as I did not get around to taking distributed systems at Princeton.

(A line I used a bunch to identify network partition sims was this:
```fmt.Printf("[KILL] %v - %v (Candidate, Term 4) - DISCONNECTED\n", time.Now().Format("15:04:05.000"), (leader+1)%servers)```
)
- Lab 1: MapReduce: ~3 hours
- Lab 2: Raft
    - 2A (leader election): ~5.5 hours
    - 2B (log): ~4 hours
    - 2C (persistence): ~0.75 hours
    - 2D (log compaction): ~1.5 hours so far