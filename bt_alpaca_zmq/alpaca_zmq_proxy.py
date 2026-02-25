import asyncio
import zmq
from alpaca.data.enums import DataFeed
from alpaca.data.live import StockDataStream, CryptoDataStream
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
import threading
from collections import defaultdict
import time
import uuid
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import pickle
import datetime

API_KEY = (
    os.environ.get('ALPACA_API_KEY')
    or os.environ.get('ALPACA_KEY')
)
SECRET_KEY = (
    os.environ.get('ALPACA_SECRET_KEY')
    or os.environ.get('ALPACA_SECRET')
)
FEED = os.environ.get("ALPACA_DATA_FEED", "sip")

if not API_KEY or not SECRET_KEY:
    raise RuntimeError(
        "Chiavi Alpaca mancanti nel proxy: imposta ALPACA_API_KEY/ALPACA_SECRET_KEY "
        "oppure ALPACA_KEY/ALPACA_SECRET"
    )

class AlpacaSmartProxy:
    def __init__(self, log_level):
        # Configurazione logging
        self._setup_logging(log_level)
        self.logger = logging.getLogger('AlpacaProxy')
        
        self.context = zmq.Context()

        # Socket configuration
        try:
            self.router = self.context.socket(zmq.ROUTER)
            self.router.setsockopt(zmq.ROUTER_HANDOVER, 1)  # Importante!
            self.router.setsockopt(zmq.ROUTER_MANDATORY, 1)
            self.router.setsockopt(zmq.IMMEDIATE, 1)
            self.router.bind("tcp://*:5555")
            self.publisher = self.context.socket(zmq.PUB)
            self.publisher.bind("tcp://*:5556")
        except zmq.ZMQError as e:
            self.logger.critical(f"Errore inizializzazione socket ZMQ: {e}")
            raise

        self.heartbeats = {}

        # Data structures
        self.client_assets = defaultdict(set)
        self.asset_subscribers = defaultdict(set)
        self.active_alpaca_symbols = set()
        self.active_alpaca_crypto_symbols = set()

        # Aggiungi queste strutture dati
        self.client_daily_assets = defaultdict(set)
        self.daily_asset_subscribers = defaultdict(set)
        self.active_alpaca_daily_symbols = set()
        if FEED == "sip":
            DATA_FEED = DataFeed.SIP
        elif FEED == "iex":
            DATA_FEED = DataFeed.IEX
        else:
            DATA_FEED = DataFeed.SIP
        # Alpaca connection
        try:
            self.alpaca_stream = StockDataStream(API_KEY, SECRET_KEY, feed=DATA_FEED)
            self.alpaca_crypto_stream = CryptoDataStream(API_KEY, SECRET_KEY)
            self._stock_hist_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
            self._crypto_hist_client = CryptoHistoricalDataClient(API_KEY, SECRET_KEY)
            self._stock_hist_client._session.verify = False
            self._crypto_hist_client._session.verify = False
            self._data_feed = DATA_FEED
        except Exception as e:
            self.logger.critical(f"Errore connessione Alpaca: {e}")
            raise

        self.running = False
        self.cleanup_thread = None
        self.alive_log_interval_s = 300
        self._last_alive_log = 0.0

        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._stream_ready_event = threading.Event()
        self._stopping = False

    def _broadcast_eod(self, reason="proxy_stop"):
        """Invia EOD broadcast a tutti i subscriber."""
        eod_msg = {
            'type':        'EOD',
            'daily':       False,
            'asset_class': 'stock',
            'symbol':      '*',
            'data':        None,
            'timestamp':   time.time(),
            'reason':      reason,
        }
        self.publisher.send_pyobj(eod_msg)
        self.logger.info(f"EOD broadcast inviato (reason={reason})")

    def _get_client_id_str(self, client_id):
        """Converti client_id in stringa leggibile"""
        try:
            if isinstance(client_id, bytes):
                return client_id.hex()[:8]
            elif isinstance(client_id, str):
                return client_id[:8]
            else:
                return str(client_id)[:8]
        except Exception:
            return "UNKNOWN_ID"

    def _safe_remove_client(self, client_id):
        """Rimozione client con gestione ID robusta"""
        if client_id not in self.heartbeats:
            client_id_str = self._get_client_id_str(client_id)
            self.logger.debug(f"Tentativo rimozione client inesistente: {client_id_str}")
            return
        
        client_id_str = self._get_client_id_str(client_id)
        try:
            # Rimuovi tutte le sottoscrizioni intraday
            if client_id in self.client_assets:
                for symbol in list(self.client_assets.get(client_id, set())):
                    self.asset_subscribers[symbol].discard(client_id)
                    if not self.asset_subscribers[symbol]:
                        try:
                            if symbol in self.active_alpaca_crypto_symbols:
                                self.alpaca_crypto_stream.unsubscribe_bars(symbol)
                                self.active_alpaca_crypto_symbols.remove(symbol)
                            else:
                                self.alpaca_stream.unsubscribe_bars(symbol)
                                self.active_alpaca_symbols.remove(symbol)
                            self.logger.info(f"Rimosso simbolo intraday {symbol} (nessun client)")
                        except Exception as e:
                            self.logger.error(f"Errore rimozione intraday {symbol}: {e}")
            # Rimuovi tutte le sottoscrizioni giornaliere
            if client_id in self.client_daily_assets:
                for symbol in list(self.client_daily_assets.get(client_id, set())):
                    self.daily_asset_subscribers[symbol].discard(client_id)
                    if not self.daily_asset_subscribers[symbol]:
                        try:
                            self.alpaca_stream.unsubscribe_daily_bars(symbol)
                            self.active_alpaca_daily_symbols.remove(symbol)
                            self.logger.info(f"Rimosso simbolo giornaliero {symbol} (nessun client)")
                        except Exception as e:
                            self.logger.error(f"Errore rimozione giornaliera {symbol}: {e}")

            else:
                self.logger.debug(f"Nessun simbolo registrato per: {client_id_str}")
            
            # Pulisci strutture dati
            for registry in [self.client_assets, self.client_daily_assets, self.heartbeats]:
                if client_id in registry:   
                    del registry[client_id]
            
            self.logger.info(f"Client {client_id_str} rimosso correttamente")
            
        except Exception as e:
            self.logger.error(f"Errore rimozione {client_id_str}: {e}")


    def _setup_logging(self, log_level=logging.INFO):
        """Configura il sistema di logging avanzato"""
        logger = logging.getLogger('AlpacaProxy')
        logger.setLevel(log_level)
        
        # Formattatore
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        
        logger.addHandler(console_handler)

    # Opzione 2: usa una funzione wrapper esplicita
    async def _alpaca_callback(self, bar):
        await self._on_bar(bar)
        
    async def _alpaca_daily_callback(self, bar):
        await self._on_bar(bar,daily=True)

    async def _alpaca_crypto_callback(self, bar):
        await self._on_bar(bar, daily=False, asset_class='crypto')

    def _add_subscription(self, client_id, symbol, timeframe=None, asset_class='stock'):
        """Gestisce una nuova sottoscrizione con supporto asincrono"""
        try:
            is_crypto = asset_class == 'crypto'
            if timeframe == 'daily':
                if is_crypto:
                    raise ValueError("Sottoscrizione daily crypto non supportata in live stream")
                prev_count = len(self.daily_asset_subscribers.get(symbol, set()))
                registry = self.client_daily_assets
                subscribers = self.daily_asset_subscribers
                active_symbols = self.active_alpaca_daily_symbols
                alpaca_method = self.alpaca_stream.subscribe_daily_bars
                callback = self._alpaca_daily_callback
                sub_type = "daily"
            else:
                prev_count = len(self.asset_subscribers.get(symbol, set()))
                registry = self.client_assets
                subscribers = self.asset_subscribers
                if is_crypto:
                    active_symbols = self.active_alpaca_crypto_symbols
                    alpaca_method = self.alpaca_crypto_stream.subscribe_bars
                    callback = self._alpaca_crypto_callback
                    sub_type = "intraday-crypto"
                else:
                    active_symbols = self.active_alpaca_symbols
                    alpaca_method = self.alpaca_stream.subscribe_bars
                    callback = self._alpaca_callback
                    sub_type = "intraday"



            
            # Inizializza strutture dati se necessarie
            if client_id not in registry:
                registry[client_id] = set()
                
            # Aggiungi sottoscrizioni
            registry[client_id].add(symbol)
            subscribers.setdefault(symbol, set()).add(client_id)
            self.heartbeats[client_id] = time.time()

            self.logger.debug(f"Client {self._get_client_id_str(client_id)} sottoscritto a {symbol} ({sub_type})")

            # Sottoscrivi ad Alpaca solo se primo client
            if prev_count == 0 and symbol not in active_symbols:
                try:
                    # Modifica chiave: usa una lambda che richiami la coroutin
                    alpaca_method(callback, symbol)
                    active_symbols.add(symbol)
                    self.logger.info(f"Sottoscritto a nuovo simbolo: {symbol}")
                    # Abilita l'avvio dello stream al primo simbolo registrato
                    self._stream_ready_event.set()
                except Exception as e:
                    self.logger.exception(e)
                    self.logger.error(f"Errore sottoscrizione Alpaca per {symbol}: {e}")
                    # Ripulisci in caso di errore
                    subscribers[symbol].discard(client_id)
                    registry[client_id].discard(symbol)
                    raise

        except Exception as e:
            self.logger.error(f"Errore in _add_subscription: {e}")
            raise


    async def _on_bar(self, bar, daily=False, asset_class='stock'):
        """Callback asincrono per dati in tempo reale"""
        try:
            subscribers = self.daily_asset_subscribers  if daily else self.asset_subscribers
            symbol = bar.symbol

            def _symbol_key(value):
                return str(value).replace("/", "").upper()

            matched_symbol = symbol
            if matched_symbol not in subscribers and asset_class == 'crypto':
                stream_key = _symbol_key(symbol)
                for sub_symbol in subscribers.keys():
                    if _symbol_key(sub_symbol) == stream_key:
                        matched_symbol = sub_symbol
                        break

            if matched_symbol not in subscribers:
                return
                
            msg = {
                'daily' : daily, 
                'asset_class': asset_class,
                'symbol': matched_symbol,
                'data': bar,
                'timestamp': time.time()
            }

            if subscribers[matched_symbol]:
                self.publisher.send_pyobj(msg)
                daily_str = "daily" if daily else "intraday"
                self.logger.debug(f"Inviato dato {matched_symbol} {daily_str} a {len(subscribers[matched_symbol])} client")
                
        except Exception as e:
            self.logger.error(f"Errore in _on_bar: {e}")


    def _cleanup_dead_clients(self):
        """Pulizia client inattivi con logging più informativo"""
        while self.running:
            time.sleep(10)  # Controlla ogni 10 secondi
            try:
                now = time.time()
                if now - self._last_alive_log >= self.alive_log_interval_s:
                    total_clients = len(self.heartbeats)
                    total_intraday = len(self.active_alpaca_symbols) + len(self.active_alpaca_crypto_symbols)
                    total_daily = len(self.active_alpaca_daily_symbols)
                    total_symbols = total_intraday + total_daily
                    self.logger.info(
                        f"Alive: {total_clients} client, {total_intraday} intraday + "
                        f"{total_daily} daily = {total_symbols} titoli"
                    )
                    self._last_alive_log = now
                dead_clients = []
                
                for client_id, last_hb in list(self.heartbeats.items()):
                    client_id_str = client_id.hex()[:8] if hasattr(client_id, 'hex') else str(client_id)[:8]
                    if now - last_hb > 30:  # Timeout 30 secondi
                        dead_clients.append(client_id)
                        self.logger.warning(f"Client [{client_id_str}] inattivo da {int(now - last_hb)}s ")
                    else:
                        self.logger.debug(f"Client [{client_id_str}] attivo (ultimo hb {int(now - last_hb)}s fa)")
                
                for client_id in dead_clients:
                    pass
                    self._safe_remove_client(client_id)
                    
            except Exception as e:
                self.logger.error(f"Errore in cleanup: {e}")


    def _handle_client_messages(self):
        """Gestione messaggi client con gestione ID robusta"""
        while self.running:
            try:
                parts = self.router.recv_multipart()
                if len(parts) == 3:
                    client_id, b, message = parts
                elif len(parts) == 2:
                    client_id, message = parts
                    b = b""
                else:
                    self.logger.warning(f"Messaggio con formato errato: {parts}")
                    continue
                
                client_id_str = self._get_client_id_str(client_id)
                
                # Aggiorna subito l'heartbeat
                self.heartbeats[client_id] = time.time()
                try:
                    msg_str = message.decode('utf-8', errors='replace') if isinstance(message, bytes) else message
                    msg_str = msg_str.strip()
                    self.logger.debug(f"Messaggio UTF-8 da {client_id_str} decodificato {msg_str}")
                except Exception as e:
                    msg_str = f"BINARY_DATA[{len(message)}]"
                    self.logger.debug(f"Messaggio binario da {client_id_str} decodificato {msg_str}")
                
                if msg_str.upper() == 'HEARTBEAT':
                    self.logger.debug(f"Heartbeat da {client_id_str}")
                    self.router.send_multipart([client_id, b"", b"PONG"])
                
                elif msg_str.startswith("MIN:") or message.startswith(b"SUB:"): #retro compatibilità
                    symbol = msg_str.split(':')[1] if ':' in msg_str else 'UNKNOWN'
                    self._add_subscription(client_id, symbol)
                    self.router.send_multipart([client_id, b"", b"ACK"])
                    self.logger.info(f"Sottoscrizione intraday {symbol} da {client_id_str}")

                elif msg_str.startswith("MIN_CRYPTO:"):
                    symbol = msg_str.split(":", 1)[1] if ":" in msg_str else "UNKNOWN"
                    self._add_subscription(client_id, symbol, asset_class='crypto')
                    self.router.send_multipart([client_id, b"", b"ACK-CRYPTO"])
                    self.logger.info(f"Sottoscrizione intraday crypto {symbol} da {client_id_str}")

                elif msg_str.startswith("DAY_CRYPTO:"):
                    self.router.send_multipart([client_id, b"", b"ERR:DAY_CRYPTO_NOT_SUPPORTED"])
                    self.logger.warning(f"Richiesta DAY_CRYPTO non supportata da {client_id_str}")
                
                elif msg_str.startswith("DAY:"):
                    symbol = msg_str.split(':')[1] if ':' in msg_str else 'UNKNOWN'
                    self._add_subscription(client_id, symbol, timeframe='daily')
                    self.router.send_multipart([client_id, b"", b"ACK-DAILY"])
                    self.logger.info(f"Sottoscrizione giornaliera {symbol} da {client_id_str}")

                elif msg_str == "DISCONNECT":
                    self._safe_remove_client(client_id)
                    self.router.send_multipart([client_id, b"", b"BYE"])
                    self.logger.info(f"Disconnessione pulita {client_id_str}")
                elif msg_str.startswith("HISTORICAL:") or msg_str.startswith("HISTORICAL|"):
                    payload = self._handle_historical_request(msg_str)
                    self.router.send_multipart([client_id, b"", pickle.dumps(payload)])
                else:
                    self.logger.warning(f"----Messaggio non riconosciuto da {client_id_str}: {msg_str[:100]}")
            except zmq.ZMQError as e:
                self.logger.error(f"Errore ZMQ: {e}", exc_info=True)
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Errore generico: {e}", exc_info=True)

    def _parse_proxy_datetime(self, value):
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.datetime.fromisoformat(value)

    def _to_timeframe(self, timeframe):
        mapping = {
            "minute": TimeFrame.Minute,
            "minutes": TimeFrame.Minute,
            "1min": TimeFrame.Minute,
            "day": TimeFrame.Day,
            "days": TimeFrame.Day,
            "1day": TimeFrame.Day,
            "week": TimeFrame.Week,
            "month": TimeFrame.Month,
        }
        key = timeframe.strip().lower()
        if key not in mapping:
            raise ValueError(f"timeframe non supportato: {timeframe}")
        return mapping[key]

    def _get_historical_data(self, symbol, timeframe, fromdate, todate, limit, asset_class="stock"):
        tf = self._to_timeframe(timeframe)
        start_dt = self._parse_proxy_datetime(fromdate)
        end_dt = self._parse_proxy_datetime(todate)

        if asset_class == "crypto":
            request = CryptoBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf,
                start=start_dt,
                end=end_dt,
                limit=limit,
            )
            barset = self._crypto_hist_client.get_crypto_bars(request)
        else:
            request = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf,
                start=start_dt,
                end=end_dt,
                limit=limit,
                feed=self._data_feed,
            )
            barset = self._stock_hist_client.get_stock_bars(request)

        return barset.df

    def _handle_historical_request(self, msg_str):
        try:
            # Nuovo formato: HISTORICAL|asset|symbol|timeframe|from|to|limit
            if "|" in msg_str:
                parts = msg_str.split("|")
                if len(parts) != 7 or parts[0] != "HISTORICAL":
                    raise ValueError(f"Formato HISTORICAL non valido: {msg_str}")
                _, asset_class, symbol, timeframe, fromdate, todate, limit = parts
                limit = int(limit)
            else:
                # Retro-compatibilità con vecchio formato stock
                _, symbol, timeframe, fromdate, todate = msg_str.split(":")
                asset_class = "stock"
                limit = 1000

            df = self._get_historical_data(
                symbol=symbol,
                timeframe=timeframe,
                fromdate=fromdate,
                todate=todate,
                limit=limit,
                asset_class=asset_class,
            )
            return {"ok": True, "df": df}
        except Exception as e:
            emsg = str(e)
            if "401" in emsg and "Authorization" in emsg:
                emsg = (
                    "401 Authorization Required su storico Alpaca dal proxy. "
                    "Verifica chiavi del processo proxy (ALPACA_API_KEY/ALPACA_SECRET_KEY "
                    "o ALPACA_KEY/ALPACA_SECRET)."
                )
            self.logger.error(f"Errore richiesta storico proxy: {emsg}")
            return {"ok": False, "error": emsg, "df": None}

    def start(self):
        """Avvia il servizio proxy"""
        self.logger.info("Avvio proxy Alpaca...")
        self.running = True
        # Resetta l'eventuale stato precedente (es. riavvii consecutivi)
        self._stream_ready_event.clear()
        
        try:
            self.cleanup_thread = threading.Thread(target=self._cleanup_dead_clients, daemon=True)
            self.cleanup_thread.start()
            
            client_thread = threading.Thread(target=self._handle_client_messages, daemon=True)
            client_thread.start()
            
            self.logger.info("Proxy pronto. In attesa di connessioni...")
            self.logger.debug("In attesa della prima sottoscrizione prima di avviare lo stream Alpaca...")
            while self.running:
                if self._stream_ready_event.wait(timeout=5):
                    break
                self.logger.debug("Idle: nessun simbolo sottoscritto, attendo... (ancora in ascolto)")

            if not self.running:
                self.logger.info("Arresto richiesto prima dell'avvio dello stream Alpaca")
                return

            self.logger.info("Prima sottoscrizione ricevuta: avvio stream Alpaca stock+crypto")
            stock_thread = threading.Thread(target=self.alpaca_stream.run, daemon=True)
            crypto_thread = threading.Thread(target=self.alpaca_crypto_stream.run, daemon=True)
            stock_thread.start()
            crypto_thread.start()
            while self.running:
                time.sleep(1)
            
        except Exception as e:
            self.logger.critical(f"Errore durante l'avvio: {e}")
            self.stop()
            raise

    def stop(self, eod_reason="proxy_stop"):
        """Arresta il servizio proxy"""
        if self._stopping:
            self.logger.info("stop() già in corso, ignoro chiamata duplicata")
            return
        self._stopping = True
        self.logger.info("Arresto proxy in corso...")
        self.running = False
        # Garantisci che eventuali thread in attesa vengano sbloccati
        self._stream_ready_event.set()

        # Nuovo comportamento: invia sempre EOD allo stop del proxy
        # per consentire shutdown ordinato dei consumer.
        try:
            self._broadcast_eod(reason=eod_reason)
            time.sleep(0.5)  # breve finestra per consentire consegna ai subscriber
        except Exception as e:
            self.logger.warning(f"EOD broadcast fallito in stop(): {e}")
        
        try:
            if self.cleanup_thread:
                self.cleanup_thread.join(timeout=5)
            
            # Disconnessione pulita da Alpaca (intraday)
            for symbol in list(self.active_alpaca_symbols):
                try:
                    self.alpaca_stream.unsubscribe_bars(symbol)
                    self.logger.info(f"Disconnesso da {symbol} su Alpaca")
                except Exception as e:
                    self.logger.error(f"Errore disconnessione {symbol}: {e}")
            for symbol in list(self.active_alpaca_crypto_symbols):
                try:
                    self.alpaca_crypto_stream.unsubscribe_bars(symbol)
                    self.logger.info(f"Disconnesso da {symbol} (crypto) su Alpaca")
                except Exception as e:
                    self.logger.error(f"Errore disconnessione crypto {symbol}: {e}")

            # Disconnessione pulita da Alpaca (daily)
            for symbol in list(self.active_alpaca_daily_symbols):
                try:
                    self.alpaca_stream.unsubscribe_daily_bars(symbol)
                    self.logger.info(f"Disconnesso da {symbol} (daily) su Alpaca")
                except Exception as e:
                    self.logger.error(f"Errore disconnessione daily {symbol}: {e}")

            self.alpaca_stream.stop_ws()
            self.alpaca_crypto_stream.stop_ws()

            self.context.destroy()
            self.logger.info("Proxy arrestato correttamente")
            
        except Exception as e:
            self.logger.critical(f"Errore durante l'arresto: {e}")
            raise

if __name__ == "__main__":
    import argparse
    import signal as _signal

    parser = argparse.ArgumentParser()
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Livello di logging')

    args = parser.parse_args()

    log_level = getattr(logging, args.log_level)
    proxy = AlpacaSmartProxy(log_level)

    def _handle_sigusr1(signum, frame):
        """
        SIGUSR1 = segnale di fine giornata controllata.
        Invia EOD a tutti i client e poi si ferma.

        Uso:
            systemctl --user kill -s USR1 zmq-proxy
            # oppure:
            kill -USR1 <pid>
        """
        proxy.logger.info("SIGUSR1: invio EOD ai client e arresto controllato...")
        proxy.stop(eod_reason="eod_manual")

    _signal.signal(_signal.SIGUSR1, _handle_sigusr1)

    def _handle_stop_signal(signum, frame):
        sname = _signal.Signals(signum).name
        proxy.logger.info(f"Segnale {sname} ricevuto: stop proxy con EOD")
        proxy.stop(eod_reason="proxy_stop")

    _signal.signal(_signal.SIGTERM, _handle_stop_signal)
    _signal.signal(_signal.SIGINT, _handle_stop_signal)

    try:
        proxy.start()
    except KeyboardInterrupt:
        proxy.logger.info("Ricevuto CTRL+C, arresto...")
        proxy.stop(eod_reason="proxy_stop")
    except Exception as e:
        proxy.logger.critical(f"Errore irreversibile: {e}")
        proxy.stop(eod_reason="proxy_error")
