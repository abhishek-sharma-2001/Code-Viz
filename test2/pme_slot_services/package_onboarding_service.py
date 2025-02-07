import aiohttp
from src.utils.logging_util import logger
from fastapi.exceptions import HTTPException
import asyncio
from src.services.pme_slot_services.pme_slots_config import config


class PackageOnboardingService:

    def __init__(self):
        self.MAX_RETRIES = int(config.MAX_RETRIES)

    async def call_package_onboarding_service(self, url, params):
        request_headers = await self.get_request_header()
        for attempt in range(self.MAX_RETRIES):
            try:
                logger.info(f"Initiating service call with payload: {params}")
                logger.info(f"Package Service URL: {url}")
                logger.info(f"Request Headers: {request_headers}")

                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url,
                        json=params,
                        headers=request_headers,
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as response:
                        response.raise_for_status()

                        # Read and process response
                        response_data = await response.json()
                        logger.info(f"Service response: {response_data}")

                        # Handle list responses gracefully
                        return response_data[0] if isinstance(response_data, list) else response_data

            except aiohttp.ClientResponseError as e:
                logger.error(f"HTTP error during service call: {e}")
                if attempt == 2:
                    raise HTTPException(status_code=400, detail=f"HTTP error: {e}")
                await asyncio.sleep(2)  # Wait before retrying

            except aiohttp.ClientConnectionError as e:
                logger.warning(f"Attempt {attempt + 1} failed due to connection error: {e}")
                if attempt == 2:
                    raise HTTPException(status_code=400, detail="Error communicating with the service after retries")
                await asyncio.sleep(2)  # Wait before retrying

            except asyncio.TimeoutError as e:
                logger.warning(f"Attempt {attempt + 1} timed out: {e}")
                if attempt == 2:
                    raise HTTPException(status_code=400, detail="Service call timed out after retries")
                await asyncio.sleep(2)  # Wait before retrying

            except Exception as error:
                logger.error(f"Failed to call package onboarding service: {error}")
                if attempt == 2:
                    raise HTTPException(status_code=400, detail="Unexpected error occurred")
                await asyncio.sleep(2)  # Wait before retrying

    @staticmethod
    async def get_request_header():
        headers = {
            "Content-Type": "application/json",
        }
        return headers