import scrapy.item.Item as Item
import scrapy.item.Field as Field

class Reference(Item):

    url = Field()
    name = Field()


class Navigation(Item):

    url = Field()
    name = Field()
