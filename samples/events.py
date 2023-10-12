from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from eventmsg_adaptor.schema import PydanticEventBody


class BusinessCategory(BaseModel):
    code: str
    name: str
    group: str


class BusinessOwner(BaseModel):
    first_name: str
    last_name: str
    email: Optional[str] = None
    mobile_number: Optional[str] = None


class BusinessRegistered(PydanticEventBody):
    business_id: UUID
    trading_name: str
    category: BusinessCategory
    legal_entity_type: str
    referral_code: Optional[str] = None
    reseller_code: Optional[str] = None
    owner: BusinessOwner

    @property
    def event_name(self) -> str:
        return "business_registered"


business_registered_event = BusinessRegistered(
    business_id=UUID("2e8a249a-001c-4880-a8b0-0a84b2811a59"),
    trading_name="Mat's Cookies",
    category=BusinessCategory(code="54321", name="Restaurant", group="Hospitality"),
    legal_entity_type="sole_prop",
    referral_code="foo",
    reseller_code="foo",
    owner=BusinessOwner(
        first_name="Mr",
        last_name="Business",
        email="mr.business@example.com",
        mobile_number="+254700000000",
    ),
)
