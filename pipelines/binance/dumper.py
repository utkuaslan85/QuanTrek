import nats
from nats.js.api import StreamInfo
import asyncio
import json
from pathlib import Path
from datetime import datetime, timedelta, time, timezone
from nats.js.api import ConsumerConfig, DeliverPolicy
import logging
from dataclasses import dataclass, asdict
from typing import Callable, Optional

logging.basicConfig(
    filename="/mnt/vol1/logs/dumper.log",  # Ensure this directory exists
    level=logging.INFO,
    format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

NATS_URL = "nats://localhost:4222"

@dataclass
class Metadata:
    stream: str = None
    subject: str = None
    symbol: str = None
    field: str = None  # New field to track which field this metadata belongs to
    created_at: str = None
    last_record_time: str = None
    
class StreamParquetConsumer():
    """Class for dumping NATS JetStream messages to parquet"""
    def __init__(self, 
                 base_path:str,
                 *,
                 resolution: str = "1s",
                 durable_name:Optional[str] = None,
                 streams:list[str] = None,
                 subject_pattern:list[str] = None,
                 nats_url: str = None,
                 interval: timedelta = timedelta(days=1),
                 transformer: Optional[list[Callable]] = None):
        self.subject_pattern = subject_pattern
        self.base_path = Path(base_path)
        self.nats_url = nats_url or NATS_URL
        self.streams = streams
        self.interval = interval
        self.resolution = resolution
        self.transformers = transformer or {}
        self.config = None
        self.durable_name = durable_name
        print("created")
        
    async def subscription(self, stream:str, subject:str, policy_type, start_time=None):
        if policy_type == 'by_start_time':
            cc = ConsumerConfig(deliver_policy=DeliverPolicy.BY_START_TIME, 
                                    opt_start_time=start_time,
                                    #opt_start_time='2025-09-20T13:16:36.510279+00:00', 
                                    #opt_start_seq=700001
                                    )
        elif policy_type == 'all':
            cc = ConsumerConfig(deliver_policy=DeliverPolicy.ALL)
        elif policy_type == 'last_per_subject':
            cc = ConsumerConfig(deliver_policy=DeliverPolicy.LAST_PER_SUBJECT)
        
        # self.nc = await nats.connect(servers=[self.nats_url])
        return await self.js.subscribe(stream=stream, 
                                       subject=subject, 
                                       config=cc,
                                       durable=self.durable_name)
           
    async def get_streams_info(self, streams:list[str] = None) -> dict[str, StreamInfo]:
        streams_info = {}
        
        if streams is None:       
            try:
                infos = await self.js.streams_info()
                for info in infos:
                    streams_info[info.config.name] = info
            except Exception as e:
                logger.error(f"❌ Failed to retrieve all streams: {e}")
                raise ValueError(f"Failed to retrieve streams list from NATS: {e}")
        else:
            # Check each stream individually for better error messages
            for stream in streams:
                try:
                    info = await self.js.stream_info(stream)
                    streams_info[stream] = info
                    logger.info(f"✅ Found stream '{stream}' with {info.state.messages} messages")
                except Exception as e:
                    # Log the specific stream that failed
                    logger.error(f"❌ Stream '{stream}' not found: {e}")
                    
                    # Try to get available streams for a helpful error message
                    try:
                        available_streams = []
                        infos = await self.js.streams_info()
                        available_streams = [info.config.name for info in infos]
                        
                        raise ValueError(f"Stream '{stream}' does not exist. "
                                    f"Available streams: {available_streams}")
                    except Exception as list_error:
                        raise ValueError(f"Stream '{stream}' does not exist and couldn't retrieve available streams: {list_error}")
        
        return streams_info
    
    async def get_subject_patterns(self, streams:list[str] = None, subject_pattern:list[str] = None) -> dict[str, list]:
        patterns: dict[str, list] = {}
        if subject_pattern is not None:
            if len(subject_pattern) > 1:
                print("Only single subject pattern can be selected")
            elif streams is None:
                print("Subject pattern cannot be selected without selecting its stream")
            elif len(streams) > 1:
                print("Only single stream can be selected if subject pattern is declared")
            else:
                patterns[streams[0]] = [subject_pattern][0]
                return patterns
        else:
            streams_info = await self.get_streams_info(streams)
            # print("all streams info")
            for stream_name, info in streams_info.items():
                # print(f"{stream_name} -> {info.config.subjects}")
                patterns[stream_name] = info.config.subjects
            # print(patterns)
            return patterns

    async def get_last_msg_info(self, stream:str, subject_pattern:str) -> dict[str, dict[str, str]]:
        """If subject list is not specified, all subjects based on
        subject pattern in the stream are auto-retrieved. \n
        returns {stream: {subject: symbol}} \n
        ex: {'binance_depth': {'binance.depth.ethbtc': 'ETHBTC', \n
                               'binance.depth.btcusdt': 'BTCUSDT'}}"""
        
        # cc = ConsumerConfig(deliver_policy=DeliverPolicy.LAST_PER_SUBJECT)
        # self.js = 
        # sub = await self.js.subscribe(stream=self.stream, subject=self.subject_pattern, config=cc)
        sub = await self.subscription(stream, subject_pattern, policy_type='last_per_subject')
        TIMEOUT_SECONDS = 5
        subject_info = {}
        subjects_info = {}
        message_count = 0
        # print("last_msg_info_called")
        async def message_collector():
            nonlocal message_count
            async for msg in sub.messages:
                logger.info(f'Received message from {msg.subject}')
                print(f'Received message from {msg.subject}')
                message_count += 1
                try:
                    data = json.loads(msg.data.decode())
                    # print(data.keys())
                    symbol = data['symbol']
                    subject_info[msg.subject] = symbol
                    subjects_info[stream] = subject_info
                    await msg.ack()
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        
        # Create task and set timeout
        task = asyncio.create_task(message_collector())
        
        try:
            await asyncio.wait_for(task, timeout=TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            task.cancel()
            logger.info("Message collection timed out")
            try:
                await task  # Wait for cancellation to complete
            except asyncio.CancelledError:
                pass
        finally:
            await sub.unsubscribe()
        # print(f"get_last_msg -> {subjects_info}")
            # Simple check: if no messages received, raise error
        if message_count == 0:
            raise ValueError(f"No messages found for subject pattern '{subject_pattern}' in stream '{stream}'.\n"
                             f"This could mean:\n"
                             f"1) Pattern doesn't match any subjects\n"
                             f"2) No recent messages for matching subjects\n"
                             f"3) Stream is empty.")
    
        logger.info(f"Successfully collected {message_count} messages")
        
        return subjects_info
     
    async def get_subjects_info(self) -> list[dict[str, dict[str, str]]]:
        """Returns [{stream: {subject: symbol}] \n
        ex: [{'binance_depth': {'binance.depth.ethbtc': 'ETHBTC',\n
                                'binance.depth.btcusdt': 'BTCUSDT'}},\n 
             {'binance_kline': {'binance.kline.btcusdt': 'BTCUSDT'}}]
        """
                               
        subject_patterns = await self.get_subject_patterns(self.streams, self.subject_pattern)
        subjects_info = []
        for stream_name, patterns_list in subject_patterns.items():
            # print("getting subjects info")
            # print(f"stream_name: {stream_name}")
            # print(f"patterns_list: {patterns_list}")
            for single_pattern in patterns_list:
                # print(f"single_pattern: {single_pattern}")
                subject_info = await self.get_last_msg_info(stream_name, single_pattern)
                # print(f"subject info: {subjects_info}")
                subjects_info.append(subject_info)
                
        return subjects_info
    
    def _get_base_path(self, stream: str, symbol: str) -> Path:
        """Single source of truth for base path structure"""
        return self.base_path / stream / symbol / self.resolution

    def _get_field_path(self, stream: str, symbol: str, field_path: str) -> Path:
        """Get path for a specific field"""
        return self._get_base_path(stream, symbol) / field_path

    def create_symbol_path(self, stream: str, symbol: str):
        path = self._get_base_path(stream, symbol)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_metadata_file(self, symbol_path: str) -> Optional[Metadata]:
        """Load metadata from file, return None if doesn't exist"""
        metadata_path = (self.base_path / symbol_path / "metadata.json")
        
        try:
            with open(metadata_path, "r") as f:
                _meta = json.load(f)
            return Metadata(**_meta)
        except FileNotFoundError:
            logger.info(f"Metadata file not found: {metadata_path}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in metadata file: {e}")
            return None
        except Exception as e:
            logger.error(f"Error reading metadata file: {e}")
            return None
    
    def save_metadata_file(self, metadata: Metadata, symbol_path: str) -> bool:
        """Save metadata to file"""
        metadata_path = (self.base_path / symbol_path / "metadata.json")
        
        try:
            # Ensure directory exists
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(metadata_path, "w") as f:
                # Use asdict to convert dataclass to dict
                json.dump(asdict(metadata), f, indent=4)
            logger.info(f"Metadata saved to {metadata_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving metadata file: {e}")
            raise e
    
    async def create_metadata(self, stream:str, symbol:str):
        """Create or load metadata for a stream/symbol combination"""
        symbol_path = self.create_symbol_path(stream, symbol)
        
        # Try to load existing metadata
        metadata = self.get_metadata_file(symbol_path)
        
        if metadata is not None:
            logger.info(f"Loaded existing metadata for {stream}/{symbol}")
            return metadata
        
        # Create new metadata
        logger.info(f"Creating new metadata for {stream}/{symbol}")
        metadata = Metadata()
        
        # Fill basic information
        metadata.stream = stream
        metadata.symbol = symbol
        metadata.created_at = datetime.now(tz=timezone.utc).isoformat()
        
         # Get subject information
        try:
            subjects_list = await self.get_subjects_info()
            
            # Find the subject for this symbol
            for stream_info in subjects_list:
                for stream_name, subjects in stream_info.items():
                    if stream_name == stream:
                        for subject, subject_symbol in subjects.items():
                            if subject_symbol == symbol:
                                metadata.subject = subject
                                break
                        break
                if metadata.subject:  # Break outer loop if found
                    break
                    
        except Exception as e:
            logger.error(f"Error getting subjects info: {e}")
            # Continue with partial metadata
        
        # Save the metadata file
        self.save_metadata_file(metadata, symbol_path)
        
        return metadata
    
    async def update_metadata(self, metadata: Metadata, symbol_path: str, **updates) -> Metadata:
        """Update specific fields in metadata"""
        for key, value in updates.items():
            if hasattr(metadata, key):
                setattr(metadata, key, value)
            else:
                logger.warning(f"Metadata doesn't have attribute: {key}")
        
        # Save updated metadata
        self.save_metadata_file(metadata, symbol_path)
        
        return metadata
    
    async def run(self):
        try:
            self.nc = await nats.connect(servers=[self.nats_url])
            self.js = self.nc.jetstream()
        except Exception as e:
            print(f"❌ Failed to connect to NATS: {e}")
            raise  # Re-raise so caller knows about connection failure
        try:
            subjects_list = await self.get_subjects_info()
        except Exception as e:
            print(f"❌ Failed to get streams info: {e}")
            raise      
        try:
            for stream_info in subjects_list:
                for stream, subjects in stream_info.items():
                    for subject, symbol in subjects.items():
                        await self.process_symbol_daily_batches(stream, subject, symbol)
            print("Batching sucessful")
        except Exception as e:
            print(f"❌ Failed to process: {e}")
            raise

    async def stream_messages(self, stream: str, subject: str, start_time: str = None):
        """Stream messages - handles None start_time for cold start"""
        
        if start_time is None:
            # Cold start - process from beginning
            sub = await self.subscription(stream, subject, policy_type='all')
            logger.info(f"Cold start streaming for {subject} from beginning")
        else:
            # Resume from specific time
            sub = await self.subscription(stream, subject, policy_type='by_start_time', start_time=start_time)
            logger.info(f"Resuming streaming for {subject} from {start_time}")
        
    # ... rest of method unchanged
        try:
            last_msg = await self.js.get_last_msg(stream, subject)
            last_data = json.loads(last_msg.data.decode())
            last_ts = last_data['timestamp']
            
            logger.info(f"Streaming started for {subject} from {start_time}")
            
            async for msg in sub.messages:
                try:
                    raw_data = json.loads(msg.data.decode())
                    msg_ts = raw_data['timestamp']
                    msg_symbol = raw_data['symbol']
                    
                    # Yield message data instead of accumulating
                    yield {
                        'raw_data': raw_data,
                        'subject': msg.subject,
                        'timestamp': msg_ts,
                        'symbol': msg_symbol
                    }
                    
                    # Check termination conditions
                    if normalize_timestamp(msg_ts) >= normalize_timestamp(last_ts):
                        logger.info(f"{msg_symbol}: Last message reached, stopping collection")
                        break
                    # elif normalize_timestamp(msg_ts) >= self.cutoff_interval().timestamp():
                    #     logger.info(f"Cutoff time reached, stopping stream")
                    #     break
                    
                    await msg.ack()
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in message streaming: {e}")
        finally:
            await sub.unsubscribe()

    def add_transformer(self, func: Callable):
        """Add a transformer to the chain"""
        self.transformers[func.__name__] = func
        return self  # For method chaining
    
    async def transformer_caller(self, name):
        transformer = self.transformers[name]
        if asyncio.iscoroutinefunction(transformer):
            return await transformer
        else:
            return transformer

    def add_config(self,config: dict[dict[list[dict[str, str]]]]):
        """{stream:{subject:[{field:{transformer:field_path}}]}}"""
        self.config = config    
    
    async def process_symbol_daily_batches(self, stream, subject, symbol):
        """Process with per-field metadata tracking"""
        
        # Get field configurations
        if self.config and stream in self.config and subject in self.config[stream]:
            fields = self.config[stream][subject]
            use_transformers = True
        else:
            # For no-config case, don't use transformer system
            use_transformers = False
            fields = [{"raw_data": {"no_transform": "data"}}]  # Just for iteration
        
            
        # Load metadata for each field and determine earliest start time
        field_metadata = {}
        earliest_start_time = None
        
        # In process_symbol_daily_batches, when determining start time:
        for field_dict in fields:
            field_name, info = list(field_dict.items())[0]
            transformer_name, field_path = list(info.items())[0]
            
            # Load field-specific metadata (handles cold start)
            metadata = await self.get_field_metadata(stream, symbol, field_path, subject)
            if metadata.subject is None:
                metadata.subject = subject
            
            field_metadata[field_path] = {
                'metadata': metadata,
                'field_name': field_name,
                'transformer_name': transformer_name,
                'batch': [],
                'current_date': None,
                'last_processed_time': None
            }
            
            # For cold start, last_record_time will be None
            field_start_time = metadata.last_record_time
            if field_start_time is None:
                # Cold start - this field needs to process from beginning
                logger.info(f"Cold start for field {field_path} - processing from beginning")
            
            # Find earliest start time (None means start from beginning)
            if earliest_start_time is None or field_start_time is None:
                earliest_start_time = field_start_time  # Could be None for cold start
            elif field_start_time < earliest_start_time:
                earliest_start_time = field_start_time
        
        # Stream messages starting from earliest required time
        async for msg_data in self.stream_messages(stream, subject, earliest_start_time):
            msg_timestamp = normalize_timestamp(msg_data['raw_data']['timestamp'])
            msg_timestamp_iso = datetime.fromtimestamp(msg_timestamp, tz=timezone.utc).isoformat()
            msg_date = datetime.fromtimestamp(msg_timestamp, tz=timezone.utc).date()
            
            # Process each field
            for field_path, field_info in field_metadata.items():
                # Skip if this field has already processed this message
                if (field_info['metadata'].last_record_time and 
                    msg_timestamp_iso <= field_info['metadata'].last_record_time):
                    continue
                
                # Transform the field
                # transformer_func = await self.transformer_caller(field_info['transformer_name'])
                
                if use_transformers:
                    # Use transformer system
                    transformer_func = await self.transformer_caller(field_info['transformer_name'])
                    if asyncio.iscoroutinefunction(transformer_func):
                        transformed_data = await transformer_func(msg_data['raw_data'][field_info['field_name']])
                    else:
                        transformed_data = transformer_func(msg_data['raw_data'][field_info['field_name']])
                else:
                    # No transformation - use raw data
                    transformed_data = msg_data['raw_data'].copy()
                        
                # Check if we need to dump this field's batch (new day)
                if field_info['current_date'] is None:
                    field_info['current_date'] = msg_date
                elif field_info['current_date'] != msg_date:
                    # Dump current batch for this field
                    if field_info['batch']:
                        await self.dump_daily_batch(
                            stream, symbol, field_info['current_date'], 
                            field_info['batch'], field_path
                        )
                        # Update field metadata after successful dump
                        if field_info['last_processed_time']:
                            await self.update_field_metadata(
                                stream, symbol, field_path, 
                                field_info['last_processed_time']
                            )
                        field_info['batch'] = []
                    field_info['current_date'] = msg_date
                
                # Prepare record
                if isinstance(transformed_data, dict):
                    record = transformed_data.copy()
                else:
                    record = {field_info['field_name']: transformed_data}
                
                record.update({
                    'timestamp': msg_timestamp,
                    'date': msg_date,
                    'symbol': symbol,
                })
                
                # Add to this field's batch
                field_info['batch'].append(record)
                field_info['last_processed_time'] = msg_timestamp_iso
        
        # Final dump and metadata update for all fields
        for field_path, field_info in field_metadata.items():
            if field_info['batch']:
                await self.dump_daily_batch(
                    stream, symbol, field_info['current_date'],
                    field_info['batch'], field_path
                )
            
            # Update field metadata with final timestamp
            if field_info['last_processed_time']:
                await self.update_field_metadata(
                    stream, symbol, field_path, 
                    field_info['last_processed_time']
                )
        
    def _create_new_field_metadata(self, stream, symbol, field_path, subject=None):
        """Create new field metadata object"""
        return Metadata(
            stream=stream,
            subject=subject,  # Set subject during creation
            symbol=symbol,
            field=field_path,
            created_at=datetime.now(tz=timezone.utc).isoformat(),
            last_record_time=None
        )

    def _get_metadata_path(self, stream: str, symbol: str, field_path: str) -> Path:
        """Get metadata file path"""
        return self._get_field_path(stream, symbol, field_path) / "metadata.json"

    async def get_field_metadata(self, stream, symbol, field_path, subject=None):
        """Get metadata for specific field - handles cold start"""
        field_metadata_path = self._get_metadata_path(stream, symbol, field_path)
        
        try:
            if field_metadata_path.exists():
                with open(field_metadata_path, "r") as f:
                    _meta = json.load(f)
                return Metadata(**_meta)
            else:
                # Cold start - create new metadata with subject
                logger.info(f"Creating new field metadata for {stream}/{symbol}/{field_path}")
                metadata = self._create_new_field_metadata(stream, symbol, field_path, subject)
                # Save immediately so subject is persisted
                await self.save_field_metadata(metadata, stream, symbol, field_path)
                return metadata
        except Exception as e:
            logger.error(f"Error reading field metadata {field_metadata_path}: {e}")
            return self._create_new_field_metadata(stream, symbol, field_path, subject)

    async def update_field_metadata(self, stream, symbol, field_path, last_record_time):
        """Update last_record_time for specific field"""
        metadata = await self.get_field_metadata(stream, symbol, field_path)
        metadata.last_record_time = last_record_time
        await self.save_field_metadata(metadata, stream, symbol, field_path)
        return metadata
    
    async def save_field_metadata(self, metadata: Metadata, stream, symbol, field_path):
        """Save field-specific metadata - creates directories if needed"""
        field_metadata_path = self._get_metadata_path(stream, symbol, field_path)
        
        try:
            # Ensure all parent directories exist
            field_metadata_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(field_metadata_path, "w") as f:
                json.dump(asdict(metadata), f, indent=4)
            logger.info(f"Field metadata saved: {field_metadata_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving field metadata {field_metadata_path}: {e}")
            raise e

    async def dump_daily_batch(self, stream: str, symbol: str, date, batch_messages, field_path:str="data"):
        """Dump daily batch to partitioned parquet files"""
        if not batch_messages:
            return

        logger.info(f"Dumping {len(batch_messages)} messages for {symbol} on {date}")

        parquet_path = self.create_parquet_path(stream, symbol, field_path, date)
        await self.append_to_parquet_with_dedup(parquet_path, batch_messages)
 
    async def append_to_parquet_with_dedup(self, file_path:Path, new_data):
        """Append to parquet with deduplication and time-based filtering"""
        import pandas as pd
        
        if not new_data:
            return
        
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        df_new = pd.DataFrame(new_data)
        
        if file_path.exists():
            # Read existing data
            df_existing = pd.read_parquet(file_path)
            
            # Combine and deduplicate
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['timestamp'], keep='last')
            df_combined = df_combined.sort_values('timestamp')

        else:
            df_combined = df_new.sort_values('timestamp')
        
        df_combined.to_parquet(file_path, index=False, compression='snappy')
        logger.info(f"Saved {len(df_new)} new records to {file_path} (total: {len(df_combined)})")

    def create_parquet_path(self, stream, symbol, field_path, date):
        return (
            self._get_field_path(stream, symbol, field_path) /
            f"year={date.year}" / f"month={date.month:02d}" / f"day={date.day:02d}" / "data.parquet"
        )
    
    async def shutdown(self):
        await self.nc.close()

    def cutoff_interval(self, ts):
            tz=timezone.utc
            # 1. Convert timestamp → datetime in the given timezone
            dt = datetime.fromtimestamp(ts, tz)

            # 2. Go back one day
            prev_day = dt.date() - self.interval

            # 3. Build datetime for 23:59:59 of that previous day
            cutoff_dt = datetime.combine(prev_day, time(23, 59, 59), tzinfo=tz)

            # 4. Return as timestamp
            return cutoff_dt
       
    async def test_method(self, stream, subject, start_time):
        try:
            self.nc = await nats.connect(servers=[self.nats_url])
            self.js = self.nc.jetstream()
        except Exception as e:
            print(f"❌ Failed to connect to NATS: {e}")
            raise  # Re-raise so caller knows about connection failure
        try:
            return await self.buffer_msgs(stream, subject, start_time)
        except Exception as e:
            print(f"❌ Failed to get streams info: {e}")
            raise


def normalize_timestamp(timestamp):
    """Convert milliseconds to seconds if needed"""
    if len(str(int(timestamp))) > 10:  # Milliseconds have 13 digits, seconds have 10
        return timestamp / 1000
    return timestamp
