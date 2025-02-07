import json
import aiohttp
import asyncio
from src.services.pme_slot_services.pme_slots_config import config
from src.utils.logging_util import logger
from fastapi.exceptions import HTTPException


class ListingService:
    def __init__(self):
        self.listing_service_url = config.LISTING_SERVICE_URL
        self.MAX_RETRIES = int(config.MAX_RETRIES)

    async def call_listing_service(self, url, params):
        retries = 0
        while retries < self.MAX_RETRIES:
            try:
                request_header = await self.get_request_header()
                logger.info(f"call_listing_service Request Header : {request_header}")
                request_body = json.dumps(params)
                logger.info(f"call_listing_service Request Body : {request_body}")
                logger.info(f"Listing Service URL : {url}")

                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url, headers=request_header, data=request_body, timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        response.raise_for_status()

                        logger.info(f"Listing Service Response: {await response.text()}")
                        return await response.json()

            except aiohttp.ClientResponseError as e:
                retries += 1
                logger.error(f"HTTP error: {e}")
                if retries == self.MAX_RETRIES:
                    raise HTTPException(status_code=400, detail=f"Error from Listing service after {self.MAX_RETRIES} retries: {str(e)}")
                await asyncio.sleep(2)  # Wait before retrying

            except aiohttp.ClientConnectionError as e:
                retries += 1
                logger.warning(f"Connection error on attempt {retries}: {e}")
                if retries == self.MAX_RETRIES:
                    raise HTTPException(status_code=400, detail="Connection error while calling Listing Service")
                await asyncio.sleep(2)  # Wait before retrying

            except asyncio.TimeoutError as e:
                retries += 1
                logger.warning(f"Timeout error on attempt {retries}: {e}")
                if retries == self.MAX_RETRIES:
                    raise HTTPException(status_code=500, detail="Timeout error while calling Listing Service")
                await asyncio.sleep(2)  # Wait before retrying

            except Exception as e:
                retries += 1
                logger.error(f"Unexpected error: {e}")
                if retries == self.MAX_RETRIES:
                    raise HTTPException(status_code=400, detail="Unexpected error occurred while calling Listing "
                                                                "Service")
                await asyncio.sleep(2)  # Wait before retrying

        raise HTTPException(status_code=400, detail="Failed to call Listing Service after multiple retries")

    async def call_pme_list_service(self, params):
        request_params = {
            "entity": "partners_booking_info",
            "filter": [
                {
                    "type": "service_type",
                    "values": "PME"
                },
                {
                    "type": "tenant",
                    "values": params.get("tenant_id", "manipal")
                },
                {
                    "type": "external_facility_code",
                    "values": params.get("external_facility_id", "")
                },
                {
                    "type": "internal_facility_id",
                    "values": params.get("facility_id", "")
                }
            ],
            "list_length": 1,
            "start_from_entry": 0
        }
        return await self.call_listing_service(self.listing_service_url, request_params)

    @staticmethod
    async def get_request_header():
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        return headers