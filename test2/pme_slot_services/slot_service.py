import json
import datetime
import time
from datetime import date
from sqlalchemy import text
from src.services.pme_slot_services.redis_client import RedisClient
from src.db.pg_connect import DatabaseConnection
from src.services.pme_slot_services.bifrost_service import BifrostService
from src.services.pme_slot_services.listing_service import ListingService
from src.services.pme_slot_services.package_onboarding_service import PackageOnboardingService
from src.services.pme_slot_services.response_mapper import ResponseMapper
from src.services.pme_slot_services.pme_slots_config import config
from fastapi.exceptions import HTTPException
from src.utils.logging_util import logger

# Initialize Redis client instance
redis_instance = RedisClient()


class SlotService:
    GENDER = config.GENDER
    TENANT_IDS = config.TENANT_IDS
    PROVIDERS_FINAL_SLOT_TIME = datetime.datetime.strptime(config.PROVIDERS_FINAL_SLOT_TIME, "%H:%M").time()
    VACCINATION_GET_SLOT_CACHE_TIME = config.VACCINATION_GET_SLOT_CACHE_TIME
    NO_OF_SLOT_DAYS_TO_BE_ADDED_FOR_PME = config.NO_OF_SLOT_DAYS_TO_BE_ADDED_FOR_PME
    RCP_PME_BOOKING = config.RCP_PME_BOOKING
    SLOT_MANAGED_BY_JHH_TENANT = config.SLOT_MANAGED_BY_JHH_TENANT

    async def get_all_slots_function(self, request):
        """Main function to get all slots based on the request."""
        start_time = time.perf_counter()  # Start measuring time
        logger.info("Starting get_all_slots_function with request: %s", request)

        try:
            if request.get("is_slots_managed_by_jhh") == "true":
                logger.info("Handling JHH managed slots case")
                env_key = str(self.SLOT_MANAGED_BY_JHH_TENANT)

                sql_query = f"""
                            SELECT env_value
                            FROM server_settings
                            WHERE env_key = '{env_key}';
                            """

                db_connector = DatabaseConnection()
                tenant_data = await db_connector.get_result_from_query(text(sql_query))
                logger.debug("Fetched tenant data: %s", tenant_data)

                if tenant_data:
                    tenant_id = tenant_data[0][0]
                    request['tenant_id'] = tenant_id
                else:
                    logger.error("Tenant ID not present in the database")
                    raise HTTPException(status_code=400, detail="Tenant ID not present in the database")
            db_time = time.perf_counter()
            logger.info("Time taken to fetch tenant data: %s ms", round((db_time - start_time) * 1000, 2))
            current_time = datetime.datetime.now().time()
            logger.debug("Current time: %s", current_time)

            response_mapper = ResponseMapper()
            if (
                    (request.get("has_api_integration") != "true" or request.get("is_slots_managed_by_jhh") == "true") and
                    datetime.datetime.strptime(request["date"], "%d-%m-%Y").date() <= datetime.date.today() + datetime.timedelta(days=1) and
                    current_time > self.PROVIDERS_FINAL_SLOT_TIME
            ):
                logger.info("Condition met for immediate slot fetch")
                slot_params = await self.get_slot_params(request)
                all_slots = await response_mapper.get_slots([], slot_params)
                response_mapper_time = time.perf_counter()
                logger.info("Time taken to get immediate slots: %s ms", round((response_mapper_time - db_time) * 1000, 2))
                return {"status": "success", "data": all_slots}

            curr_date = date.today()
            logger.debug("Current date: %s", curr_date)

            all_slots, key = await self.get_key_slot(request)
            get_key_slot_time = time.perf_counter()
            logger.info("Time taken to get key slot: %s ms", round((get_key_slot_time - db_time) * 1000, 2))
            tenant_slots, tenant_slots_key = await self.get_key_bifrost_get_slot_response(request)
            get_key_bifrost_get_slot_response_time = time.perf_counter()
            logger.info("Time taken to get key bifrost get slot response: %s ms", round((get_key_bifrost_get_slot_response_time - get_key_slot_time) * 1000, 2))

            if not all_slots and not tenant_slots:
                logger.info("No cached slots found, fetching new slots")
                slot_params = await self.get_slot_params(request)
                selected_date_str = request.get("date")
                selected_date = datetime.datetime.strptime(selected_date_str, '%d-%m-%Y')

                proposed_end_date = curr_date + datetime.timedelta(days=self.NO_OF_SLOT_DAYS_TO_BE_ADDED_FOR_PME)
                if isinstance(curr_date, datetime.datetime):
                    curr_date = curr_date.date()

                end_date = proposed_end_date if curr_date <= proposed_end_date else selected_date
                slot_params['end_date'] = end_date.strftime('%d-%m-%Y')

                bifrost_service = BifrostService()
                tenant_slots = await bifrost_service.get_slots(slot_params)
                bifrost_service_time = time.perf_counter()
                logger.info("Time taken to fetch slots from Bifrost: %s ms", round((bifrost_service_time - get_key_bifrost_get_slot_response_time) * 1000, 2))

                await self.set_redis_slot_cache(tenant_slots_key, tenant_slots)
                set_redis_slot_cache_time = time.perf_counter()
                logger.info("Time taken to set redis slot cache: %s ms", round((set_redis_slot_cache_time - bifrost_service_time) * 1000, 2))
                current_time = datetime.datetime.now().time()
                tomorrow = (datetime.datetime.now() + datetime.timedelta(days=1)).date()

                if isinstance(tenant_slots, list):
                    slot_available = None
                    next_available_date = None
                    result = tenant_slots
                elif isinstance(tenant_slots, dict):
                    slot_available = tenant_slots.get("slot_available", None)
                    next_available_date = tenant_slots.get("next_available_date", None)
                    result = tenant_slots.get("result", [])
                else:
                    slot_available = None
                    next_available_date = None
                    result = []
                request_date = datetime.datetime.strptime(request['date'], '%d-%m-%Y').date()
                if (request.get('has_api_integration') != 'true' or request.get(
                        'is_slots_managed_by_jhh') == 'true') and \
                        request_date <= tomorrow and \
                        current_time > self.PROVIDERS_FINAL_SLOT_TIME:
                    # Immediate slot fetch for today or tomorrow after cutoff time
                    all_slots = await response_mapper.get_slots([], slot_params, slot_available, next_available_date)
                    response_mapper_time = time.perf_counter()
                    logger.info("Time taken to get immediate slots: %s ms", round((response_mapper_time - set_redis_slot_cache_time) * 1000, 2))
                # elif request_date > tomorrow:
                #     # Handle slots for future dates
                #     logger.info("Handling future date slot request")
                #     all_slots = await response_mapper.get_slots(result, slot_params, slot_available,
                #                                                 next_available_date)
                #     response_mapper_time = time.perf_counter()
                #     logger.info("Time taken to get future slots: %s ms", round((response_mapper_time - set_redis_slot_cache_time) * 1000, 2))
                else:
                    # Default case
                    all_slots = await response_mapper.get_slots(result, slot_params, slot_available,
                                                                next_available_date)
                    response_mapper_time = time.perf_counter()
                    logger.info("Time taken to get slots: %s ms", round((response_mapper_time - set_redis_slot_cache_time) * 1000, 2))

                await self.set_redis_slot_cache(key, all_slots)
                set_redis_slot_cache_time = time.perf_counter()
                logger.info("Time taken to set redis slot cache: %s ms", round((set_redis_slot_cache_time - response_mapper_time) * 1000, 2))

            # Ensure to close the Redis connection
            await redis_instance.close()

            return {"status": "success", "data": all_slots}
        finally:
            end_time = time.perf_counter()  # Stop measuring time
            execution_time = round((end_time - start_time) * 1000, 2)
            logger.info("Time Taken for Total execution time of get_all_slots_function: %s ms", execution_time)

    async def set_redis_slot_cache(self, tenant_slots_key, tenant_slots):
        """Set the Redis cache for tenant slots if key is present."""
        logger.debug("Setting Redis cache with key: %s", tenant_slots_key)
        if tenant_slots_key:
            # redis_client = await redis_instance.get_client()
            await redis_instance.set_json(tenant_slots_key, json.dumps(tenant_slots), ex=self.VACCINATION_GET_SLOT_CACHE_TIME,
                             nx=True)

    @staticmethod
    async def get_key_slot(request):
        key = f"tenant-slots-{request['date']}-{request['tenant_id']}-{request['user_corporate_id']}-{request['external_facility_id']}-{request['service_type_id']}-{request['facility_id']}-{request['external_facility_location']}"
        logger.info(f"======= SlotsController.get_key_slot caching key is : {key} =============")
        # redis_client = await redis_instance.get_client()
        all_slot = await redis_instance.get_json(key)
        all_slot = json.loads(all_slot) if all_slot else None
        return all_slot, key

    @staticmethod
    async def get_key_bifrost_get_slot_response(request):
        key = f"tenant-slots-bifrost-response-{request['date']}-{request['tenant_id']}-{request['user_corporate_id']}-{request['external_facility_id']}-{request['service_type_id']}-{request['facility_id']}-{request['external_facility_location']}"
        logger.info(f"======= SlotsController.get_key_bifrost_get_slot_response caching key is : {key} =============")
        # redis_client = await redis_instance.get_client()
        all_slot = await redis_instance.get_json(key)
        all_slot = json.loads(all_slot) if all_slot else None
        return all_slot, key

    async def get_slot_params(self, request):
        """Construct the slot parameters based on tenant_id."""
        logger.debug("Building slot parameters for tenant_id: %s", request.get("tenant_id"))
        # if request["tenant_id"] == self.TENANT_IDS["RCP"]:
        #     rcp_booking = json.loads(self.RCP_PME_BOOKING["doctorid"])
        #     request["doctor_id"] = rcp_booking
        # elif request["tenant_id"] == self.TENANT_IDS["MANIPAL"]:
        if request["tenant_id"] == self.TENANT_IDS["RCP"] or request["tenant_id"] == self.TENANT_IDS["MANIPAL"]:
            listing_service = ListingService()
            try:
                response = await listing_service.call_pme_list_service(request)
                print("response listing service : ", response)

                if response.get("data") and isinstance(response["data"].get("filters"), list) and response["data"]["filters"]:
                    association_detail = response["data"]["filters"][-1]
                    request["doctor_id"] = association_detail.get("doctor_id")
                    if "speciality_code" in association_detail:
                        request["additional_info"] = json.dumps({
                            "speciality_code": association_detail.get("speciality_code",None)
                        })
                else:
                    raise HTTPException(status_code=400, detail="Invalid response: 'filters' missing or empty in "
                                                                "response['data']")
            except Exception as e:
                logger.error(
                    "Exception occurred in get_all_slots slot_params listing service",
                    exc_info=True  # Includes the traceback in the log
                )
                raise HTTPException(
                    status_code=400,
                    detail="Invalid response: 'filters' missing or empty in response['data']"
                )

        elif request["tenant_id"] == self.TENANT_IDS["MEDDIBUDDY"]:
            package_details = await self.get_external_package_details(request)
            if "external_package_id" in package_details:
                request["additional_info"] = json.dumps({
                    "ext_bat_package_id": package_details.get("external_package_id",None)
                })
        return request

    async def get_external_package_details(self, request):
        """Fetch external package details based on the provided parameters."""
        logger.info("Fetching external package details")
        onboarding_params = {
            "corporate": request.get("corporate_id"),
            "provider_id": request.get("provider_id"),
            "facility_id": request.get("clinic_id"),
            "internal_package_id": request.get("internal_package_id")
        }

        package_onboarding_service = PackageOnboardingService()
        resp = await package_onboarding_service.call_package_onboarding_service(config.PACKAGE_LISTING_URL, onboarding_params)

        logger.debug("Package onboarding service response: %s", resp)
        if "external_package_id" in resp:
            external_package_id = resp["external_package_id"]
            gender = request.get("gender")
            external_package_id = await self.parse_external_package_id(external_package_id, gender)
            return {
                "external_package_id": external_package_id,
                "external_package_name": resp.get("external_package_name")
            }
        else:
            logger.error("No package details found")
            raise Exception("No package details found. Onboarding service response: {}".format(resp))

    async def parse_external_package_id(self, external_package_id, gender):
        """Parse and return external package id based on gender."""
        logger.info("Parsing external package ID")
        try:
            parsed = json.loads(external_package_id)
            if parsed:
                return parsed.get("Male") if int(gender) == int(self.GENDER["male"]) else parsed.get("Female")
        except Exception as e:
            logger.error(f"**** Exception in parsing external_package_id: {e}")
        return external_package_id


slot_service = SlotService()