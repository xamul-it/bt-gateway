# Proxy A/B Blueprint

## Obiettivo
Separare il `control plane` dal ciclo caldo degli stream Alpaca per evitare che un singolo processo saturato rallenti:

- registrazione simboli
- heartbeat client
- richieste storico
- publish verso i consumer

## Processo A: Control Plane
Responsabilità:

- espone `ROUTER` pubblico su `tcp://*:5555`
- espone `PUB` pubblico su `tcp://*:5556`
- gestisce `HEARTBEAT`, `MIN:`, `DAY:`, `MIN_CRYPTO:`, `DISCONNECT`, `HISTORICAL`
- mantiene registry client/simboli
- decide quando una subscription passa `0 -> 1` o `1 -> 0`
- inoltra al worker solo i comandi di subscribe/unsubscribe
- riceve dal worker i payload live già serializzati e li repubblica ai consumer

## Processo B: Alpaca Stream Worker
Responsabilità:

- possiede `StockDataStream` e `CryptoDataStream`
- applica le subscription reali verso Alpaca
- riceve le callback live da Alpaca
- costruisce il payload `msgpack`
- invia i payload live al processo A
- gestisce lo shutdown corretto degli stream con `DataStream.stop()`

## IPC interno
Canali interni localhost:

- `A -> B`: `PUSH/PULL` comandi
- `B -> A`: `PUSH/PULL` eventi live

Formato:

- comandi: `msgpack` con `action`, `symbol`, `timeframe`, `asset_class`
- eventi live: multipart `[topic=symbol, payload=msgpack(bar)]`

## Flusso
1. il client si collega ad A su `5555`
2. A aggiorna il registry
3. se il simbolo è nuovo per quella vista, A manda `subscribe` a B
4. B sottoscrive Alpaca e avvia gli stream se non sono già partiti
5. B riceve le barre e le serializza
6. B inoltra a A il payload già pronto
7. A lo repubblica su `5556`

## Monitoraggio
Parametro comune: `--monitor-seconds N`

Ogni processo logga periodicamente:

- `pid`
- `%cpu`
- `core` corrente
- `rss_mb`
- `threads`
- `hot=true/false` se `cpu >= 90%`

Metriche extra:

- `MONITOR CTRL`: `clients`, `intraday_syms`, `daily_syms`, `worker_pid`
- `MONITOR STREAM`: `stock_syms`, `crypto_syms`, `daily_syms`, `stock_stream_started`, `crypto_stream_started`

## Compatibilità
- porte pubbliche invariate: `5555/5556`
- protocollo client invariato
- `HISTORICAL` resta in A
- nessuna modifica richiesta ai consumer

## Vantaggio atteso
Se il loop Alpaca o la libreria websocket scaldano CPU, il control plane resta reattivo e non rallenta subscribe, heartbeat e routing.
