# Breaking News Radar: lead-time study

**Question:** when something happens in the world, how soon does WikiPulse flag the
Wikipedia page as trending?

**Method** (D-031): replay 26 hours of real English Wikipedia article edits from
EventStreams (2026-11-08 18:00 UTC to 2026-11-09 20:15 UTC: 132,751 edits), run the radar
over it, then look up each trend's real-world event time in public reporting.
**Lead time = trend start - event time.** The trend start is the beginning of the first
5-minute window that met every rule (z >= 4, >= 5 edits, >= 3 editors, <= 50 % bots,
< 30 % reverts). Live, the radar evaluates a window about 2 minutes after it closes, so a
real alert would arrive about 7 minutes after the trend start.

Environment: cloud dev container, 2026-11-09, commands at the end.

## Results

| Trend start (UTC) | Page | Real-world event | Event time (UTC) | Lead time | Sources |
|---|---|---|---|---|---|
| 11-09 14:30 | Russell Brand | Sentenced at Southwark Crown Court | 14:22-14:26 (live blog) | **+4 to +8 min** after the sentence | [Far Out live blog](https://faroutmagazine.co.uk/russell-brand-live-updates-awaits-sentence-rape-sexual-assault/), [NBC News](https://www.nbcnews.com/world/united-kingdom/russell-brand-sentenced-years-british-jail-rcna607799) |
| 11-08 23:20 | 2026 NASCAR Cup Series | Season finale at Homestead; Kyle Larson champion, Ryan Blaney race winner | Race end not found; first results article 23:32 | **12 min before** that article | [Yahoo Sports / MotorBiscuit (published 23:32 UTC)](https://sports.yahoo.com/articles/ryan-blaney-wins-2026-nascar-233233621.html), [Motorsport.com](https://www.motorsport.com/nascar-cup/news/kyle-larson-wins-2026-nascar-cup-title-with-stage-1-clinch-at-homestead-/10873140/) |
| 11-09 00:10 | 2026 Las Vegas Raiders season | Lost 38-17 at the 49ers | Kickoff 4:25 PM (CBS, Eastern) = 21:25; end time not published | About at the final whistle (approximate: 2 h 45 min after kickoff; editors updated the box score at 00:14) | [CBS Sports](https://www.cbssports.com/nfl/gametracker/recap/NFL_20261108_LV@SF/), [ESPN](https://www.espn.com/nfl/game/_/gameId/401873047) |
| 11-08 21:35 | Dževad Prekazi | Death of the former Partizan/Galatasaray player | First report found: b92, 21:00 Belgrade = 20:00 | **+95 min** after the first report (Wikipedia's own "death" edit was at 21:37) | [b92](https://www.b92.net/sport/fudbal/srpski-fudbal/283321/preminuo-dzevad-prekazi/vest), [Anadolu](https://www.aa.com.tr/ba/balkan/preminuo-d%C5%BEevad-prekazi-legenda-partizana-i-galatasaraya/4116988) |
| 11-09 10:45 | 2026 Kraków shooting | Follow-up: investigation details (weapons seized, perpetrator), not the attack itself (7 Nov, before the replay) | Not found | Not measurable | [France 24](https://www.france24.com/en/live-news/20261109-what-to-know-about-the-krakow-mass-shooting), [NBC News](https://www.nbcnews.com/world/europe/poland-krakow-shooting-rcna608001) |
| 11-08 18:05 | 2026 El Ayyat train collision | Train collision in Giza, 7 dead | Date only (8 Nov); no time found | Not measurable (the trend began in the replay's 2nd window, so earlier activity is unseen) | [Wikipedia](https://en.wikipedia.org/wiki/2026_El_Ayyat_train_collision), [Egyptian Streets](https://egyptianstreets.com/2026/11/08/el-ayyat-train-collision-kills-7-injures-112-in-giza/) |
| 11-08 18:20 | Frozen yogurt | **False positive:** a misinformation edit, its revert, and cleanup | - | - | edit summaries |
| 11-09 16:15 | Red fox | **False positive:** vandalism ("bob") reverted by an anti-vandalism bot | - | - | edit summaries |

## What this shows

- **8 trends in 26 hours, 6 of them real news** (75 % precision by manual review; n = 8,
  so treat it as an early indication, not a rate).
- **When the event time is known precisely, the radar is fast.** The court sentence was
  flagged within 4-8 minutes, and the NASCAR result 12 minutes *before* the first full
  results article found.
- **WikiPulse can't be faster than Wikipedia's editors.** For the Prekazi death, editors
  only updated the article 97 minutes after the first news report, and the radar flagged it
  within 2 minutes of that edit.
- **It catches follow-ups too.** The Kraków trend was a second wave of edits two days after the
  attack, when new investigation details came out.

## Honest limits

- **Recall is unknown.** The sample is picked from the radar's own detections, so this
  measures speed when it works, not how many news events it misses.
- **Event times are approximate.** They come from news timestamps and live blogs; two of
  six couldn't be timed at all.
- **Thresholds were tuned on this same replay once.** The revert gate (< 30 % reverts) was
  added after seeing that 12 of the first 20 trends were vandalism-and-revert cycles (D-032).
  Before that rule: 20 trends, the same 6 news stories plus 14 others. A fresh replay of
  a different day is the fair test.
- **Page-view confirmation isn't in this study.** The Pageviews API was rate limited (429) from
  the cloud machine. Run `python -m wikipulse.radar --once` from a home connection after a
  replay to fill `views_status` for these trends.

## Reproduce

```bash
python -m wikipulse.producer --since 2026-11-08T18:00:00Z --reset-checkpoint --max-seconds 3000
python -m wikipulse.processor --max-idle-seconds 60
python -m wikipulse.radar --rebuild --once --no-pageviews
```

Wikimedia keeps about 10 days of stream history, so this exact window can be replayed only
until about 2026-11-18.
