from scrapy import Field
from . import BaseItem

class SouthernTownItem(BaseItem):
    application_id = Field()
    description = Field()
    application_group = Field()

    category = Field()
    sub_category = Field()

    lodgement_date = Field()
    stage = Field()
    certifier_approval_date = Field()
    
    names = Field()
    address = Field()
    documents = Field()

    class Meta:
        table = 'southern_town'
        unique_fields = ['application_id']
