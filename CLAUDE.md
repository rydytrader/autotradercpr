# TraderEdge AutoTrader — VWAP + Supertrend Options Strategy

## Project Overview
Intraday options-buying bot on NIFTY. On the first NIFTY spot tick each morning
after 09:15 IST, subscribes ±N strikes around the ATM anchor and picks the CE
and PE trading closest to a configurable target premium (default ₹250). Each
leg is monitored independently on its own 3-min chart:

- **Entry**: VWAP-bounce candle (low ≤ VWAP AND close > VWAP AND close > open)
  AND Supertrend(10, 3) is up.
- **SL**: entry candle's low. Enforced in-memory via LTP tick listener; market
  exit fires the instant LTP ≤ SL.
- **Exit trail**: Supertrend flip on the option's own chart. Unlimited re-entries
  (each fresh VWAP-bounce green bar re-arms).
- **Concurrency**: CE and PE positions can be open simultaneously.
- **Squareoff**: hard market-exit at configurable cutoff (default 15:25).

## Tech Stack
- **Backend**: Spring Boot 4.0.3, Java 17
- **Frontend**: Thymeleaf templates, vanilla JS, lightweight-charts, Bootstrap 5
- **Broker + Data**: Fyers v3 REST + two WebSockets (HSM binary market data, JSON order)
- **WebSocket lib**: Java-WebSocket 1.5.3
- **Persistence**: H2 (settings + trades), JSON files (cache), SLF4J + Logback logs
- **Build**: Maven

## Architecture

### Data pipeline (Fyers-only as of 2026-08-26)
```
Fyers HSM WebSocket (wss://socket.fyers.in/hsm/v1-5/prod)
  ↓ binary frames, FULL mode (mode byte 70 / 'F') — LTP + ATP + volume + LTT + EFT
HsmBinaryParser
  ↓ RawTick
MarketDataService.onTick → pushLtpTick → LtpListener fanout
  ├─ FyersMinuteBarBuilder — aggregates ticks into 1-min OHLC per symbol,
  │    volume = sessionVol delta within bucket, emit at minute boundary
  │    ↓ appendOneMinBar
  │  CandleAggregator (1-min ring per symbol, session VWAP recomputed per bar)
  │    ↓ getHistory(sym, N) aggregates 1-min → N-min buckets on demand
  └─ VwapSupertrendStrategy.onTick (SL enforcement, spot-open capture)
```

GDFL removed entirely on branch `VWAP_SUPERTREND_STRATEGY`. Fyers HSM is the
sole market-data source. Bar OHLC is built LOCALLY from ticks (Fyers has no
canonical bar push equivalent to GDFL's SubscribeSnapshot); user accepted the
tick-aggregation trade-off.

### Order pipeline
```
Fyers Order WebSocket (wss://socket.fyers.in/trade/v3)  — JSON, orders/trades/positions
  ↓
OrderEventService.onOrderEvent — captures fills, cancels, modifications
  ↓ FillListener fanout
VwapSupertrendStrategy.onOrderFill → matches ceLeg/peLeg.entryOrderId,
  transitions PENDING_ENTRY → IN_POSITION, freezes fillPrice
```

Strategy places MARKET orders via `OrderService.placeOrder(sym, qty, 1, 0.0, "INTRADAY")`.
Exits via `placeExitOrder(sym, qty, -1, "INTRADAY")`.

### Strategy FSM
```
BOOT
  ↓ (first NIFTY spot tick ≥ 09:15 IST)
STRIKES_SUBSCRIBING  (subscribe ±N strikes for both CE and PE via
  ↓                   marketDataService.subscribeAdditional)
ARMED
  ↓ Per-leg: WAITING → PENDING_ENTRY (on entry order) → IN_POSITION (on fill)
  ↓          → WAITING (on SL / ST-flip / squareoff)
DONE_FOR_DAY  (on 15:25 cutoff)
```

Pair pick happens ~15 s after strike subscription (`tick()` polls). Scans LTPs
for all 30 subscribed strikes, picks the CE nearest to `vwapStTargetPremium`
and the PE nearest to the same. Then fetches historical 1-min bars via
`fyersClient.getHistory(sym, "1", today.minusDays(3), today)` for both chosen
symbols and prepends via `candleAggregator.prependHistory` so Supertrend is
valid from BAR 1 of today's session.

### Chart page (`/chart`)
Two side-by-side panels — CE (green dot) and PE (red dot) — each showing 3-min
candles + session VWAP (yellow) + Supertrend step line (colored per latest
direction, green when up, red when down). Panels populate as soon as the
strategy picks the pair (~09:15:15 IST). Poll cadence 2 s.

`/api/chart/symbols` returns `{ ceSymbol, peSymbol, ceTick, peTick, spotOpen, atmStrike }`.
`/api/chart/candles?symbol=X` returns `{ history, stSeries, exchangeNowMs }`.

## Fyers API

### Status Codes
- `1` = Cancelled
- `2` = Traded/Filled
- `5` = Rejected
- `6` = Pending/Open

### Endpoints Used
- POST `/api/v3/orders/sync` — place order
- PUT  `/api/v3/orders/sync` — modify order
- DELETE `/api/v3/orders/sync` — cancel order
- GET `/api/v3/orders` — order book
- GET `/api/v3/positions` — positions
- GET `/api/v3/tradebook` — tradebook
- GET `/api/v3/profile` — profile
- GET `/data/quotes` — market quotes (fallback)
- GET `/data/history` — historical OHLC bars (used for Supertrend warmup)
- POST `/data/symbol-token` — HSM token resolution
- POST `/api/v3/validate-authcode` — login

### Auth Pattern
- REST header: `Authorization: clientId:accessToken`
- Order WS: same header during handshake
- Data WS: JWT → decode → `hsm_key` for binary auth

## Key Services

### MarketDataService
- Owns the Fyers HSM WebSocket lifecycle (connect, auth, subscribe, reconnect,
  401 handling, ping keepalive).
- Implements `FyersDataWebSocket.TickCallback`. `onTick(RawTick)` fans out via
  `pushLtpTick(LtpTick)` to registered `LtpListener`s.
- SSE emitter management for the browser ticker.
- `subscribeAdditional(Collection<String>)` — strategy calls this to subscribe
  option strikes on demand. `unsubscribeAdditional` for cleanup.
- LTP cache (`currentTicks`) — every ingress updates last-known LTP / open /
  high / low / prevClose / ATP.

### CandleAggregator
- Stores 1-min bars per symbol in a bounded FIFO ring.
- `appendOneMinBar(sym, bar)` — called by `FyersMinuteBarBuilder` on minute
  boundary. Recomputes pandas_ta session VWAP for every bar in the ring
  (`Σ((H+L+C)/3 × v) / Σ(v)`, cumulative from IST midnight day-key).
- `getHistory(sym, intervalMinutes)` — returns 1-min bars raw when interval=1,
  or aggregates into N-min buckets (open=first, high=max, low=min, close=last,
  volume=sum, vwap=last contributing bar's vwap).
- `subscribe(sym, listener)` — fires listener on every 1-min bar close.

### FyersMinuteBarBuilder
- Bridge between MarketDataService LTP stream and CandleAggregator.
- Per-symbol current bucket state keyed by `ExchFeedTime / 60`. On boundary
  crossing, emits the closed bucket via `appendOneMinBar`.
- Volume per bar = `endSessionVol - startSessionVol` (delta within bucket).
- 1 Hz `@Scheduled` sweep closes stale buckets on symbols that stop ticking
  mid-minute (illiquid safety net).

### OrderEventService
- Fyers Order WebSocket handler. Captures fills, cancels, modifications.
- `addFillListener(BiConsumer<orderId, price>)` — strategy hooks this to
  detect entry / exit fills.

### VwapSupertrendStrategy
- Sole strategy on this branch. Full FSM + per-leg state + persistence.
- Implements `Strategy` — auto-picked by `StrategyScheduler` (tick every 5 s)
  and `PortfolioRiskService` (portfolio-wide kill switch).

## File Structure
```
src/main/java/com/rydytrader/autotrader/
├── config/          FyersProperties, AsyncConfig, TelegramProperties
├── controller/      TradingController, ViewController, ChartController,
│                    SettingsController, StrategyHistoryController,
│                    MarketTickerController, MarketTickerSseController
├── dto/             Candle, OrderDTO, PositionsDTO, TickData, ...
├── entity/          StrategyTradeEntity (H2 row per closed trade)
├── fyers/           FyersClient, LiveFyersClient, FyersClientRouter
├── indicator/       SuperTrend, Atr, TrueRange, FloorPivots
├── manager/         PositionManager
├── service/
│   ├── strategy/    Strategy, VwapSupertrendStrategy
│   ├── MarketDataService  (Fyers HSM WS lifecycle + LTP cache + SSE)
│   ├── FyersMinuteBarBuilder  (tick→1min bar bridge)
│   ├── CandleAggregator   (1-min ring + N-min aggregation + VWAP)
│   ├── OrderService, OrderEventService, PollingService
│   ├── HistoricalChartStore, EventService, TokenStore, ...
├── store/           RiskSettingsStore, PositionStateStore, TokenStore, ...
├── util/            NiftyExpiryResolver, NiftyOptionSymbolBuilder, ...
└── websocket/       FyersDataWebSocket, HsmBinaryParser, FyersOrderWebSocket

src/main/resources/
├── templates/       chart, home, strategy, trades, calendar, trade, login
├── static/css/      shared.css (dark / light / forest themes)
├── static/js/       common.js, ticker.js, settings-modal.js, ...
├── logback-spring.xml
└── application.properties

store/
├── config/          cpr-data.json, nse-holidays.json
├── cache/           candle-aggregator-state.json, vwap-supertrend-state.json
├── data/            positions/, events/, history/, charts/
└── logs/            autotrader.log (30-day roll, 200 MB cap)
```

## Settings (VWAP + Supertrend tab in gear modal)
| Setting              | Default   | Purpose                                                  |
|----------------------|-----------|----------------------------------------------------------|
| Strategy enabled     | true      | Master kill switch                                        |
| Lots per Leg         | 1         | ×65 = qty per entry                                       |
| Squareoff Time       | 15:25     | Hard exit cutoff                                          |
| Target Premium (₹)   | 250       | Pick CE + PE nearest to this LTP                          |
| Strikes Range (±)    | 15        | Subscribe ATM ± N strikes                                 |
| Candle Minutes       | 3         | Signal timeframe                                          |
| Supertrend ATR       | 10        | ATR period                                                |
| Supertrend Multiplier| 3.0       | ATR × this = band distance                                |

## Conventions
- Event log prefixes: `[SUCCESS]`, `[WARNING]`, `[ERROR]`, `[INFO]`, `[WS]`
- Log format: `HH:mm:ss.SSS LEVEL [ClassName] message`
- Prices rounded to tick size via SymbolMasterService (loaded from Fyers CSV)
- Persistence: H2 for durable trade rows + risk settings; JSON files in
  `store/cache/` for in-memory FSM snapshots
