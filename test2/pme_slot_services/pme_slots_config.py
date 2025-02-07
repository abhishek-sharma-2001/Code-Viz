from typing import Optional
from pydantic import BaseSettings


class PmeSlotConfig(BaseSettings):
    # services/pme_slot_services below

    # Listing Service
    LISTING_SERVICE_URL: str
    MAX_RETRIES: str

    # bifrost service
    BIFROST_API_SERVER_BASE_URL: str

    # redis
    REDIS_HOST: str
    REDIS_PORT: str
    REDIS_PASSWORD: str
    REDIS_DB: str
    SECRET: str

    # Package Onboarding Service
    PACKAGE_LISTING_URL: str

    # response mapper
    MAX_SLOT_LIST_DAYS_INCLINIC: int

    # slot_service.py
    GENDER: dict
    TENANT_IDS: dict
    MISSING_EXTERNAL_FACILITY_ID: str
    MISSING_EXTERNAL_FACILITY_LOCATION: str
    MISSING_TENANT_ID: str
    MISSING_PROVIDER_ID: str
    MISSING_CLINIC_ID: str
    MISSING_CORPORATE_ID: str
    MISSING_INTERNAL_PACKAGE_ID: str
    PROVIDERS_FINAL_SLOT_TIME: str
    VACCINATION_GET_SLOT_CACHE_TIME: int
    NO_OF_SLOT_DAYS_TO_BE_ADDED_FOR_PME: int
    RCP_PME_BOOKING: dict
    SLOT_MANAGED_BY_JHH_TENANT: str
    DATABASE_URL: str


config = PmeSlotConfig()