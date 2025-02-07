import json
from datetime import datetime, timedelta
import asyncio
from typing import List, Dict, Any
from src.services.pme_slot_services.pme_slots_config import config
from src.services.pme_slot_services.redis_client import RedisClient
from src.utils.logging_util import logger

# Initialize Redis
redis_instance = RedisClient()


class ResponseMapper:

    @staticmethod
    async def get_redis_slots(params: Dict[str, Any], slots_start_and_end: List):
        """Fetch slot data from Redis asynchronously."""
        try:
            redis_keys = [
                f"PME_{start_time.strftime('%d_%m_%YT%H:%M:%S')}_"
                f"{end_time.strftime('%d_%m_%YT%H:%M:%S')}_FACILITY_{params['facility_id']}_PROVIDER_{params['provider_id']}"
                for start_time, end_time in slots_start_and_end
            ]
            logger.info(f"******* Redis keys: {redis_keys} **")
            # redis_client = await redis_instance.get_client()
            redis_slot_values = await redis_instance.mget_json(redis_keys) if redis_keys else []
            redis_values = [json.loads(value) if value else None for value in redis_slot_values]
            logger.info(f"******* Redis values: {redis_values} **")
            return redis_values
        except Exception as e:
            logger.error(f"Error fetching Redis slots: {e}")
            return []

    async def get_slots(self, res_bifrost: List[Dict[str, Any]], params: Dict[str, Any], slot_available=0,
                        next_available_date=None):
        """Retrieve and map slot data asynchronously."""
        logger.info(
            f"** Entered ResponseMapper.get_slots ** Response slots from Bifrost: {res_bifrost} ** Params: {params} **")
        try:
            today = datetime.today()
            tasks = [
                self.process_day_slots(
                    today + timedelta(days=i),
                    res_bifrost,
                    params,
                    slot_available,
                    next_available_date
                )
                for i in range(config.MAX_SLOT_LIST_DAYS_INCLINIC)
            ]

            res = await asyncio.gather(*tasks)
            return res
        except Exception as e:
            logger.error(
                f"**** Exception in ResponseMapper.get_slots ** Error: {e} ** Traceback: {e.__traceback__} **")
            return []

    async def process_day_slots(self, date: datetime, res_bifrost: List[Dict[str, Any]], params: Dict[str, Any],
                                slot_available, next_available_date):
        """Process slots for a single day asynchronously."""
        is_target_date = date.date() == datetime.strptime(params["date"], "%d-%m-%Y").date()

        if is_target_date:
            res_ele = {
                "consultation_service_type": "4",
                "date": params["date"],
                "available_slot_count": len(res_bifrost),
                "block_slot_hold_time": "5",
                "next_available_date": next_available_date,
                "slot_available": slot_available,
                "time_wise_slots_list": []
            }

            time_wise_slots_list_ele = {
                "part_of_the_day": "All Day",
                "available_slot_count": len(res_bifrost),
                "center_wise_slots_list": []
            }

            center_wise_slots_list_ele = {
                "center_name": params["facility_name"],
                "partner_consult_center_id": "",
                "consultation_fees": 0,
                "mrn_required": False,
                "patient_consent_required": False,
                "patient_consent_box_text": "",
                "partner_name": params["tenant_id"],
                "slot_list": []
            }

            slots_start_and_end = [
                (
                    datetime.strptime(f"{date.strftime('%d-%m-%Y')} {slot['slot_start_time']}",
                                      "%d-%m-%Y %I:%M %p") - timedelta(hours=5, minutes=30),
                    datetime.strptime(f"{date.strftime('%d-%m-%Y')} {slot['slot_end_time']}",
                                      "%d-%m-%Y %I:%M %p") - timedelta(hours=5, minutes=30)
                )
                for slot in res_bifrost
            ]
            redis_slots = await self.get_redis_slots(params, slots_start_and_end)

            for count, slot in enumerate(res_bifrost, start=1):
                redis_slot = redis_slots[count - 1]
                slot_status = 1 if slot["isbooked"] == 0 and (
                            not redis_slot or redis_slot.get("patient_user_id") != str(
                        params["patient_user_id"])) else 2

                slot_list_entry = {
                    "id": f"{date.strftime('%Y-%m-%d')}-{count}",
                    "start_time": slot["slot_start_time"],
                    "end_time": slot["slot_end_time"],
                    "status": slot_status,
                    "additional_info": slot.get("additional_info", {})
                }
                center_wise_slots_list_ele["slot_list"].append(slot_list_entry)

            time_wise_slots_list_ele["center_wise_slots_list"].append(center_wise_slots_list_ele)
            res_ele["time_wise_slots_list"].append(time_wise_slots_list_ele)
        else:
            res_ele = {
                "consultation_service_type": "4",
                "date": date.strftime("%d-%m-%Y"),
                "available_slot_count": 0,
                "block_slot_hold_time": "5",
                "next_available_date": "",
                "time_wise_slots_list": []
            }

        return res_ele