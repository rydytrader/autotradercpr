# TraderEdge CPR AutoTrader

## Project Overview
Automated intraday trading bot for Indian equity markets. Receives TradingView alerts via webhook, places orders on Fyers broker, manages SL/target OCO, and tracks P&L.

## Tech Stack
- **Backend**: Spring Boot 4.0.3, Java 17
- **Frontend**: Thymeleaf templates, vanilla JS, Bootstrap 5, Chart.js
- **Broker API**: Fyers v3 REST + WebSocket
- **WebSocket**: Java-WebSocket 1.5.3
- **Logging**: SLF4J + Logback (daily rolling file)
- **Persistence**: JSON files on disk (no database)
- **Build**: Maven

## Architecture

### Live-Only Mode
The app runs exclusively in LIVE mode — no simulator. All mock/simulator code has been removed.

### WebSocket Connections (LIVE mode)
1. **Market Data WebSocket** (`wss://socket.fyers.in/hsm/v1-5/prod`)
   - Binary protocol (HsmBinaryParser), lite mode (LTP only)
   - Feeds real-time ticker + position P&L via SSE to browser
   - Handles trailing SL trigger detection
   - Service: `MarketDataService`

2. **Order Update WebSocket** (`wss://socket.fyers.in/trade/v3`)
   - JSON protocol, subscribes to orders/trades/positions
   - Detects entry fills, SL/target fills, cancellations, modifications
   - Detects manual positions and external closes
   - Service: `OrderEventService`

### When both WebSockets are connected:
- Zero API polling (syncPosition skipped entirely)
- Entry fills detected via push (replaces 2s polling)
- OCO fills detected via push (replaces 5s polling)
- Manual SL/target modifications detected in real-time
- Status shows "WS CONNECTED"

### Fallback
- If WebSockets disconnect, PollingService resumes polling automatically
- syncPosition runs every 10s as safety net
- Status shows "POLLING"

### Signal Flow
```
TradingView Alert → POST /placeorder → SignalProcessor (filters/qty)
  → OrderService.placeOrder → Fyers API
  → OrderEventService tracks order ID (if WS connected)
  OR PollingService.monitorEntry (polling fallback)
  → On fill: place SL + Target
  → OrderEventService tracks OCO (if WS connected)
  OR PollingService.monitorOCO (polling fallback)
  → On SL/target fill: cancel counterpart, record trade, clear state
```

### Key Design Patterns
- **FyersClient interface** → LiveFyersClient (single implementation)
- **FyersClientRouter** → delegates to LiveFyersClient
- **PositionManager** — static in-memory LONG/SHORT/NONE per symbol
- **PositionStateStore** — JSON files on disk (store/data/positions/)
- **OrderEventService ↔ PollingService** — circular dependency avoided via `setPollingService()` setter

### SSE (Server-Sent Events)
- Single SSE connection per page (ticker.js manages `window.__tickerSSE`)
- `ticker` event — market data for scrolling ticker
- `positions` event — live LTP + P&L for open positions
- Pages reuse shared EventSource, no duplicate connections
- Fallback to REST polling if SSE disconnects

## Fyers API

### Status Codes
- `1` = Cancelled
- `2` = Traded/Filled
- `5` = Rejected
- `6` = Pending/Open

### Endpoints Used
- POST `/api/v3/orders/sync` — place order
- PUT `/api/v3/orders/sync` — modify order (trailing SL)
- DELETE `/api/v3/orders/sync` — cancel order
- GET `/api/v3/orders` — order book
- GET `/api/v3/positions` — positions
- GET `/api/v3/tradebook` — trade history
- GET `/api/v3/profile` — user profile
- GET `/data/quotes` — market quotes (fallback)
- POST `/data/symbol-token` — HSM token resolution
- POST `/api/v3/validate-authcode` — login

### Auth Pattern
- Header: `Authorization: clientId:accessToken`
- Order WS: `authorization` HTTP header during handshake
- Data WS: JWT → decode → `hsm_key` for binary auth

## File Structure
```
src/main/java/com/rydytrader/autotrader/
├── config/          AsyncConfig, FyersProperties, TelegramProperties
├── controller/      TradingController, ViewController, SimulatorController,
│                    SettingsController, MarketTickerController, MarketTickerSseController
├── dto/             OrderDTO, PositionsDTO, TickData, TradeRecord, ProcessedSignal, CprLevels, JournalMetrics
├── fyers/           FyersClient (interface), LiveFyersClient, FyersClientRouter
├── manager/         PositionManager (static)
├── service/         PollingService, OrderService, OrderEventService, MarketDataService,
│                    SignalProcessor, EventService, TradeHistoryService, BhavcopyService,
│                    MarketHolidayService, SymbolMasterService, TelegramService, LoginService,
│                    MarginDataService, QuantityService
├── store/           PositionStateStore, RiskSettingsStore, TokenStore, TradingStateStore
└── websocket/       FyersDataWebSocket, FyersOrderWebSocket, HsmBinaryParser

src/main/resources/
├── templates/       home, positions, trades, journal, settings, console, login
├── static/css/      shared.css (3 themes: dark, light, forest)
├── static/js/       common.js, ticker.js
├── logback-spring.xml
└── application.properties

src/main/pine/       TraderEdge CPR AutoTrader.txt (Pine Script indicator)

store/
├── config/              Configuration files
│   ├── risk-settings.json   Risk management settings
│   ├── cpr-data.json        Cached CPR levels from NSE bhavcopy
│   └── nse-holidays.json    Cached NSE trading holidays
├── data/                Runtime data
│   ├── positions/       Position JSON files (one per symbol)
│   ├── events/          Daily event log files
│   └── history/         Daily trade history files
└── logs/                Application logs
    └── autotrader.log   Daily rolling log (30 days, 200MB cap)
```

## Key Services

### PollingService
- Core trading engine — entry monitor, OCO monitor, position sync, squareoff
- Falls back to polling when WebSockets are down
- Guards: pendingEntrySymbols, ocoHandledSymbols, ocoMonitoredSymbols (for polling path)
- Public helpers for WS: setSymbolState, clearSymbolStateFromWs, addCachedPosition

### OrderEventService
- Handles Order WebSocket events
- Tracks entry/OCO orders by ID
- On fill: places SL/target, cancels counterpart, records trade
- Detects manual positions and external closes
- Detects SL/target price modifications

### MarketDataService
- Manages Data WebSocket lifecycle
- SSE push to browser (ticker + positions)
- Trailing SL: monitors LTP, modifies SL when trigger hit

### SignalProcessor
- Validates and filters incoming signals
- Computes targets from CPR levels
- Target shift logic, small/large candle filters
- Session move limit (day open + PDC based)
- Risk-based quantity calculation
- TradingView symbol conversion (_ to -)

## Signal Probability
Signals are classified by alignment between weekly and daily CPR trends:

| Weekly Trend | Daily Trend | Category | Default |
|-------------|-------------|----------|---------|
| Bullish | Bullish | **HPT** (High Probable Trade) | ON |
| Bearish | Bearish | **HPT** | ON |
| Neutral | Bullish | **MPT** (Medium Probable Trade) | OFF |
| Neutral | Bearish | **MPT** | OFF |
| Bearish | Bullish | **LPT** (Low Probable Trade) | OFF |
| Bullish | Bearish | **LPT** | OFF |

Each category is independently toggleable in Pine Script settings. Alert JSON includes `"probability":"HPT"`, `"MPT"`, or `"LPT"`.

## Trading Features
- **SL from fill price**: SL recalculated using actual fill price (not Pine Script close)
- **Trailing SL**: configurable trigger % and lock % (default 75%/50%)
- **Auto Square Off**: scheduled at configurable time
- **Session Move Limit**: halves qty if price moved too far from day open/PDC
- **Target Shift**: shifts to next CPR level if default target < 1 ATR
- **Small/Large Candle Filters**: reject based on ATR multiples
- **Risk Gating**: max daily loss, risk per trade, exposure limits
- **Dedup Guard**: 5-second window prevents duplicate trade recordings

## UI Pages
- **Home** — day P&L hero, equity curve, trade stats
- **Positions** — live positions table, market clock, P&L stats
- **Trade Log** — all trades with P&L
- **Journal** — win/loss analysis, profit factor
- **Settings** — all configurable parameters
- **Console** — color-coded application logs with search/filter

## Conventions
- Event log prefixes: `[SUCCESS]`, `[WARNING]`, `[ERROR]`, `[INFO]`, `[WS]`
- Log format: `HH:mm:ss.SSS LEVEL [ClassName] message`
- All settings persisted as JSON in store/config/
- Prices rounded to tick size via SymbolMasterService (loaded from Fyers CSV)
- No database — all state in JSON files (store/) and in-memory maps
