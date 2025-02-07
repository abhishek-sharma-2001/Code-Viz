from fastapi import HTTPException
from src.services.pme_slot_services.pme_slots_config import config
from src.utils.logging_util import logger
import aioredis
from typing import Optional, Any, List
import json
from asyncio import sleep, wait_for
from contextlib import asynccontextmanager
import asyncio


class RedisClient:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, host=config.REDIS_HOST, port=config.REDIS_PORT,
                 db=config.REDIS_DB, password=config.REDIS_PASSWORD, redis_operation_timeout=5):
        if not hasattr(self, '_client'):
            self._client = None
            self.host = host
            self.port = port
            self.db = db
            self.password = password
            self.redis_operation_timeout = redis_operation_timeout  # Timeout for Redis operations (in seconds)
            self.pool_settings = {
                'max_connections': 100,
                'retry_on_timeout': True,
                'health_check_interval': 30
            }
            self._connection_lock = asyncio.Lock()
            self._health_check_task = None

    async def get_client(self) -> aioredis.Redis:
        """Get Redis client."""
        async with self._connection_lock:
            if self._client is None or not await self._check_connection():
                try:
                    self._client = await aioredis.from_url(
                        f"rediss://{self.host}:{self.port}",
                        db=self.db,
                        password=self.password,
                        decode_responses=True,
                        **self.pool_settings
                    )
                    await self._client.ping()
                    logger.info("Connected to Redis successfully.")

                    if not self._health_check_task:
                        self._health_check_task = asyncio.create_task(self._periodic_health_check())

                except Exception as e:
                    logger.error(f"Redis connection failed during get_client: {e}")
                    self._client = None
                    raise HTTPException(
                        status_code=400,
                        detail=f"Redis connection initialization failed: {e}"
                    )
        return self._client

    async def _check_connection(self) -> bool:
        """Check if current connection is healthy."""
        if self._client:
            try:
                await self._client.ping()
                return True
            except Exception:
                return False
        return False

    async def _periodic_health_check(self):
        """Periodic health check for connection pool."""
        while True:
            try:
                await asyncio.sleep(30)
                if self._client and not await self._check_connection():
                    logger.warning("Redis connection unhealthy, triggering reconnect")
                    await self.reconnect()
            except Exception as e:
                logger.error(f"Health check error: {e}")

    @asynccontextmanager
    async def get_connection(self):
        """Context manager for safe connection handling."""
        try:
            client = await self.get_client()
            if not client:
                logger.error("Redis client is None.")
                raise HTTPException(status_code=400, detail="Redis client is unavailable.")
            yield client
        except Exception as e:
            logger.error(f"Redis operation failed: {e}")
            raise

    async def mget_json(self, keys: List[str], retries=3) -> List[Any]:
        """Optimized multiple key get with JSON deserialization and retries."""
        async with self.get_connection() as client:
            for attempt in range(retries):
                try:
                    start_time = asyncio.get_running_loop().time()
                    values = await wait_for(client.mget(keys), timeout=self.redis_operation_timeout)
                    elapsed_time = asyncio.get_running_loop().time() - start_time
                    logger.info(f"mget_json completed in {elapsed_time:.2f}s for keys: {keys}")
                    return [json.loads(v) if v else None for v in values]
                except json.JSONDecodeError as jde:
                    logger.error(f"JSON deserialization error in mget_json for keys: {keys}. Error: {jde}")
                    raise
                except asyncio.TimeoutError:
                    logger.warning(f"mget_json timed out for keys: {keys}. Attempt {attempt + 1}/{retries}")
                except Exception as e:
                    logger.error(f"mget_json failed for keys: {keys}. Attempt {attempt + 1}/{retries}. Error: {e}")
                    await self.reconnect()
            raise HTTPException(status_code=504, detail="mget_json operation timed out")

    async def get_json(self, key: str, retries=3) -> Optional[Any]:
        """Get and deserialize JSON value for a key, with retries."""
        async with self.get_connection() as client:
            for attempt in range(retries):
                try:
                    start_time = asyncio.get_running_loop().time()
                    value = await wait_for(client.get(key), timeout=self.redis_operation_timeout)
                    elapsed_time = asyncio.get_running_loop().time() - start_time
                    logger.info(f"get_json completed in {elapsed_time:.2f}s for key: {key}")
                    return json.loads(value) if value else None
                except json.JSONDecodeError as jde:
                    logger.error(f"JSON deserialization error in get_json for key: {key}. Error: {jde}")
                    raise
                except asyncio.TimeoutError:
                    logger.warning(f"get_json timed out for key: {key}. Attempt {attempt + 1}/{retries}")
                except Exception as e:
                    logger.error(f"get_json failed for key: {key}. Attempt {attempt + 1}/{retries}. Error: {e}")
                    await self.reconnect()
            raise HTTPException(status_code=504, detail="get_json operation timed out")

    async def set_json(self, key: str, value: Any, ex: Optional[int] = None, nx: bool = False, retries=3):
        """Optimized JSON set operation with retries."""
        async with self.get_connection() as client:
            for attempt in range(retries):
                try:
                    serialized_value = json.dumps(value)
                    start_time = asyncio.get_running_loop().time()
                    await wait_for(client.set(key, serialized_value, ex=ex, nx=nx), timeout=self.redis_operation_timeout)
                    elapsed_time = asyncio.get_running_loop().time() - start_time
                    logger.info(f"set_json completed in {elapsed_time:.2f}s for key: {key}, value: {value}")
                    return
                except json.JSONDecodeError as jde:
                    logger.error(f"JSON serialization error in set_json for key: {key}, value: {value}. Error: {jde}")
                    raise
                except asyncio.TimeoutError:
                    logger.warning(f"set_json timed out for key: {key}. Attempt {attempt + 1}/{retries}")
                except Exception as e:
                    logger.error(f"set_json failed for key: {key}, value: {value}. Attempt {attempt + 1}/{retries}. Error: {e}")
                    await self.reconnect()
            raise HTTPException(status_code=504, detail="set_json operation timed out")

    async def close(self):
        """Improved cleanup."""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        if self._client:
            await self._client.close()
            self._client = None
            logger.info("Redis connection closed.")

    async def reconnect(self, retries=3, delay=2):
        """Enhanced reconnection logic."""
        async with self._connection_lock:
            for attempt in range(retries):
                try:
                    if self._client:
                        await self.close()
                    await self.get_client()
                    return
                except Exception as e:
                    logger.warning(f"Reconnect attempt {attempt + 1}/{retries} failed: {e}")
                    if attempt < retries - 1:
                        await sleep(delay * (attempt + 1))
            raise HTTPException(
                status_code=400,
                detail="Redis reconnection failed after all retries"
            )
