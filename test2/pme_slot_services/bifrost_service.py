import hmac
import hashlib
import json
import aiohttp
from fastapi.exceptions import HTTPException
from src.services.pme_slot_services.pme_slots_config import config
from src.utils.logging_util import logger
from src.exceptions.exceptions import ApiRequestError
import asyncio


class BifrostService:
    def __init__(self):
        self.bifrost_api_server_base_url = config.BIFROST_API_SERVER_BASE_URL
        self.MAX_RETRIES = int(config.MAX_RETRIES)

    async def get_slots(self, params):
        """Fetches slots from Bifrost."""
        try:
            logger.info("----- Entered Bifrost get_slots ------")
            logger.info("Bifrost URL: %s", self.bifrost_api_server_base_url)
            request_body = await self.request_body(params)
            logger.info(f"Generated request body: {request_body}")

            secret = str(config.SECRET)
            logger.info(f"generated secret: {secret}")
            checksum = await self.generate_checksum(secret, request_body)
            logger.info(f"generated checksum: {checksum}")

            request_header = await self.request_header(params, checksum)
            logger.info(f"Generated request header: {request_header}")

            for attempt in range(self.MAX_RETRIES):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            self.bifrost_api_server_base_url,
                            headers=request_header,
                            data=json.dumps(request_body),
                            timeout=aiohttp.ClientTimeout(total=30),
                        ) as response:
                            if response.status != 200:
                                error_message = f"Error fetching data from Bifrost: {await response.text()} with status {response.status}"
                                logger.error(error_message)
                                raise ApiRequestError(error_message)

                            response_text = await response.text()
                            logger.info(f"Raw response text: {response_text}")
                            try:
                                parsed_response = json.loads(response_text)
                                logger.info(f"Parsed Response : {parsed_response}")
                                logger.info(f"Extracted response from Bifrost API : {parsed_response.get('result', {})}")
                            except json.JSONDecodeError:
                                logger.error("Invalid JSON response from Bifrost API", exc_info=True)
                                raise HTTPException(
                                    status_code=400, detail="Invalid JSON response from Bifrost API"
                                )

                            logger.info(f"Received response from Bifrost API: {parsed_response}")
                            if parsed_response.get("status_code") == 200 and parsed_response.get("status") == "success":
                                return parsed_response.get("result", {})
                            elif parsed_response.get("status_code") == 204:
                                return {}
                            else:
                                error_message = f"Unexpected response from Bifrost API: {parsed_response}"
                                logger.error(error_message)
                                raise HTTPException(status_code=400, detail=error_message)

                except aiohttp.ClientConnectionError as e:
                    logger.warning(f"Attempt {attempt + 1} failed due to connection error: {e}")
                    if attempt == self.MAX_RETRIES - 1:
                        raise HTTPException(
                            status_code=400, detail="Error communicating with Bifrost API after multiple retries"
                        )
                    await asyncio.sleep(2)  # Wait before retrying
                except Exception as e:
                    logger.error("Unexpected error during request to Bifrost API", exc_info=True)
                    raise HTTPException(status_code=400, detail="Error fetching slots from BifrostService")
        except Exception as e:
            logger.error("Unexpected error in get_slots", exc_info=True)
            raise HTTPException(status_code=400, detail="Error fetching slots from BifrostService")

    @staticmethod
    async def request_header(params, checksum):
        try:
            headers = {
                "tenantid": params.get("tenant_id", ""),
                "event": "OUT_GET_BAT_SLOT",
                "Content-Type": "application/json",
                "checksum": checksum,
                "requesttype":"sync",
                "pme-identifier":"true",
            }
            return headers
        except Exception as e:
            logger.error("Error generating request headers", exc_info=True)
            raise HTTPException(status_code=400, detail="Error generating request headers")

    @staticmethod
    async def request_body(params):
        try:
            data = {
                "clinicid": params.get("clinic_id", ""),
                "doctorid": str(params.get("doctor_id", "")).strip(),
                "appointmenttype": "PME",
                "user_corp_partner_auth_id": params.get("user_corporate_id", ""),
                "ext_health_center_id": params.get("external_facility_id", ""),
                "date": params.get("date", ""),
                "ext_health_center_location": params.get("external_facility_location", ""),
                "provider_id": params.get("provider_id", ""),
                "pincode": params.get("pincode", ""),
                "additional_info": params.get("additional_info", None),
                "end_date": params.get("end_date", ""),
            }
            tenant_id = params.get("tenant_id", "")
            metadata = {
                "interface_key": f"jhhservice_{tenant_id}_bat_slot_list" if tenant_id else "",
                "integration_name": tenant_id,
                "source": "jhhservice",
            }
            transformed_payload = {"data": data, "metadata": metadata}
            return transformed_payload
        except Exception as e:
            logger.error("Error generating request body", exc_info=True)
            raise HTTPException(status_code=400, detail="Error generating request body")

    @staticmethod
    async def generate_checksum(secret, body):
        try:
            if not isinstance(body, str):
                body = json.dumps(body)
            checksum = hmac.digest(secret.encode(), body.encode("utf-8"), hashlib.sha256).hex()
            return checksum
        except Exception as e:
            logger.error("Error generating checksum", exc_info=True)
            raise HTTPException(status_code=400, detail="Error generating checksum")