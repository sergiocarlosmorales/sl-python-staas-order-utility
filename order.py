import SoftLayer


class VolumeOrder:

    def __init__(self):
        self.client = SoftLayer.create_client_from_env()
        self.package = None
        self.vmware_customer_flag = None
        self.fileblock_beta_access_flag = None

    def is_vmware_customer(self):
        if self.vmware_customer_flag is None:
            self.vmware_customer_flag = self.client['Account'].isActiveVmwareCustomer()

        return self.vmware_customer_flag

    def has_fileblock_beta_access(self):
        if self.fileblock_beta_access_flag is None:
            account = self.client['Account'].getObject(mask='mask[fileBlockBetaAccessFlag]')
            self.fileblock_beta_access_flag = account['fileBlockBetaAccessFlag']

        return self.fileblock_beta_access_flag

    def get_package(self):
        """
        Get the package (offering) along with all the data we need.
        :return: dict resembling SoftLayer_Product_Package
        """
        if self.package is None:
            object_mask = '''
            mask[
                id,
                itemPrices[
                    id,
                    categories[categoryCode],
                    capacityRestrictionMinimum,
                    capacityRestrictionMaximum,
                    capacityRestrictionType,
                    eligibilityStrategy,
                    item[
                        attributes[
                            attributeTypeKeyName,
                            value
                        ],
                        capacity,
                        capacityMinimum,
                        capacityMaximum
                    ],
                    locationGroupId
                ]
            ]'''
            object_filter = {
                'type': {
                    'keyName': {'operation': 'STORAGE_AS_A_SERVICE'}
                }
            }
            packages = self.client['Product_Package'].getAllObjects(filter=object_filter, mask=object_mask)
            package = packages[0]
            filtered_prices = []
            vmware_customer_flag = self.is_vmware_customer()
            beta_access_flag = self.has_fileblock_beta_access()
            # Remove prices to which we don't have access to.
            for price in package['itemPrices']:
                eligibility_strategy = price.get('eligibilityStrategy')

                if eligibility_strategy == "VMWARE_CUSTOMER" and not vmware_customer_flag:
                    continue

                if eligibility_strategy == "FILE_BLOCK_BETA_ACCESS" and not beta_access_flag:
                    continue

                filtered_prices.append(price)

            package['itemPrices'] = filtered_prices
            self.package = package

        return self.package

    def order(self, size, storage_type, performance_type, performance_value, region_name):
        """
        Places an order for a storage volume.
        :param size: integer in GBs
        :param storage_type: string valid values are 'file' or 'block'
        :param performance_type: string valid values are 'iops' or 'tier
        :param performance_value: int
        :param region_name: string for a region name or int for a SoftLayer_Location id
        :return: dict resembling SoftLayer_Container_Product_Order_Receipt
        """
        package = self.get_package()
        order_container = {
            'complexType': 'SoftLayer_Container_Product_Order_Network_Storage_AsAService',
            'packageId': package['id'],
            'location': region_name,
            'volumeSize': size,
            'prices': []
        }

        if storage_type == 'block':
            order_container['osFormatType'] = {
                'keyName': 'VMWARE'
            }

        if performance_type == 'iops':
            order_container['iops'] = performance_value

        order_container['prices'].append(self.get_storage_service_price())
        order_container['prices'].append(self.get_storage_type_price(storage_type))
        order_container['prices'].append(self.get_storage_space_price(size, performance_type, performance_value))
        order_container['prices'].append(self.get_performance_price(size, performance_type, performance_value))
        return self.place_order(order_container)

    def get_storage_service_price(self):
        """
        Get the price to be used for the 'service' item. This is a $0 price.
        :return: dict resembling SoftLayer_Product_Item_Price
        """
        return self.get_standard_price_for_category('storage_as_a_service')

    def get_standard_price_for_category(self, category_code):
        """
        Get the standard price (no location group) for a given category code.
        If we send a standard price, and the location requires a location price, the API does the switch automatically.
        So lets leave it up to the API.
        :param category_code: string
        :return: dict resembling SoftLayer_Product_Item_Price
        """
        return self.get_first_price(self.get_standard_prices_for_category(category_code))

    def get_standard_prices_for_category(self, category_code):
        """
        Get the standard prices for a given category code.
        :param category_code:
        :return: list of dicts, each element resembling SoftLayer_Product_Item_Price
        """
        package = self.get_package()
        all_prices = package['itemPrices']
        standard_prices = list(
            filter(lambda price: (price['locationGroupId'] is None) or (price['locationGroupId'] == ''), all_prices)
        )

        return list(filter(lambda price: self.is_price_for_category(price, category_code), standard_prices))

    @staticmethod
    def is_price_for_category(price, category_code):
        """
        Determine if the given price is assigned to the category_code.
        :param price: dict resembling SoftLayer_Product_Item_Price
        :param category_code: string
        :return: bool
        """
        if 'categories' in price:
            for category in price['categories']:
                if category['categoryCode'] == category_code:
                    return True
        return False

    def get_storage_type_price(self, storage_type):
        """
        Get the price for the storage type (file or block). This is a $0 price.
        :param storage_type: string
        :return: dict resembling SoftLayer_Product_Item_Price
        """
        category_code = 'block' if storage_type == 'storage_block' else 'storage_file'
        return self.get_standard_price_for_category(category_code)

    def get_performance_price(self, size, performance_type, performance_value):
        """
        Get the price for the given performance type & value.
        :param size: int
        :param performance_type: string
        :param performance_value: int
        :return: dict resembling SoftLayer_Product_Item_Price
        """
        category_code = 'performance_storage_iops' if performance_type == 'iops' else 'storage_tier_level'
        performance_prices = self.get_standard_prices_for_category(category_code)
        price_for_performance_value = None
        if performance_type == 'tier':
            prices_for_tier = self.filter_prices_for_performance_tier(performance_prices, performance_value)
            price_for_performance_value = self.get_first_price(prices_for_tier)

        if performance_type == 'iops':
            prices_for_capacity = self.filter_prices_by_product_capacity_for_value(
                performance_prices,
                performance_value
            )
            prices_for_iops_value = self.filter_prices_by_capacity_restrictions_for_value(
                prices_for_capacity,
                size
            )
            price_for_performance_value = self.get_first_price(prices_for_iops_value)

        return price_for_performance_value

    @staticmethod
    def filter_prices_for_performance_tier(prices, tier_level):
        """
        Get only the prices that are for a given tier level.
        :param prices: list of dicts, each element resembling SoftLayer_Product_Item_Price
        :param tier_level: int
        :return: list of dicts, each element resembling SoftLayer_Product_Item_Price
        """
        matching_prices = []
        for price in prices:
            for attribute in price['item']['attributes']:
                if (attribute['attributeTypeKeyName'] == 'STORAGE_TIER_LEVEL') \
                        and (int(attribute['value']) == tier_level):
                    matching_prices.append(price)

        return matching_prices

    def get_storage_space_price(self, size, performance_type, performance_value):
        """
        Get the price for storage space that is compatible for the provided performance values.
        :param size: int
        :param performance_type: string
        :param performance_value: int
        :return: dict resembling SoftLayer_Product_Item_Price
        """
        storage_space_prices = self.get_standard_prices_for_category('performance_storage_space')
        storage_space_prices_for_performance_type = self.filter_prices_with_capacity_restriction_type(
            storage_space_prices,
            self.get_capacity_restriction_type_for_performance_type(performance_type)
        )
        storage_space_prices_for_performance_value = self.filter_prices_by_capacity_restrictions_for_value(
            storage_space_prices_for_performance_type,
            performance_value
        )
        storage_space_prices_for_size = self.filter_prices_by_product_capacity_for_value(
            storage_space_prices_for_performance_value,
            size
        )

        return self.get_first_price(storage_space_prices_for_size)

    @staticmethod
    def filter_prices_by_product_capacity_for_value(prices, value):
        """
        Get the prices whose product item capacity is in range for the given value.
        :param prices: list of dicts, each element resembling SoftLayer_Product_Item_Price
        :param value: int
        :return: list of dicts, each element resembling SoftLayer_Product_Item_Price
        """
        matches = []
        for price in prices:
            item = price['item']
            capacity_minimum = int(item['capacityMinimum'])
            capacity_maximum = int(item['capacityMaximum'])
            if (capacity_minimum <= value) and (capacity_maximum >= value):
                matches.append(price)
        return matches

    @staticmethod
    def filter_prices_with_capacity_restriction_type(prices, capacity_restriction_type):
        """
        Get the prices that have a particular capacityRestrictionType
        :param prices: list of dicts, each element resembling SoftLayer_Product_Item_Price
        :param capacity_restriction_type: string
        :return: list of dicts, each element resembling SoftLayer_Product_Item_Price
        """
        return list(
            filter(lambda price: price['capacityRestrictionType'] == capacity_restriction_type, prices)
        )

    def filter_prices_by_capacity_restrictions_for_value(self, prices, value):
        """
        Get the prices that match the capacity restrictions (range) for a given value.
        :param prices: list of dicts, each element resembling SoftLayer_Product_Item_Price
        :param value: int
        :return: list of dicts, each element resembling SoftLayer_Product_Item_Price
        """
        return list(
            filter(lambda price: self.is_value_within_capacity_restrictions(price, value), prices)
        )

    @staticmethod
    def is_value_within_capacity_restrictions(price, value):
        """
        Determine if the provided value falls between the capacity restrictions for the given price.
        :param price: dict resembling SoftLayer_Product_Item_Price
        :param value: int
        :return: bool
        """
        capacity_restriction_minimum = int(price['capacityRestrictionMinimum'])
        capacity_restriction_maximum = int(price['capacityRestrictionMaximum'])
        return (capacity_restriction_minimum <= value) and (capacity_restriction_maximum >= value)

    @staticmethod
    def get_capacity_restriction_type_for_performance_type(performance_type):
        """
        Get the capacity restriction type to use, according to the API,
        based on our local naming convention (iops or tier).
        :param performance_type: string
        :return: string
        """
        if performance_type == 'iops':
            return 'IOPS'

        if performance_type == 'tier':
            return 'STORAGE_TIER_LEVEL'

        raise ValueError('Invalid performance type, must be either: iops or tier')

    @staticmethod
    def get_first_price(prices):
        """
        Syntactic sugar to get the first price in a list.
        :param prices:
        :return:
        """
        return prices[0]

    def place_order(self, order_container):
        """
        Send the provided order container to the API endpoint
        :param order_container: dict resembling SoftLayer_Container_Product_Order_Network_Storage_AsAService
        :return: dict resembling SoftLayer_Container_Product_Order_Receipt
        """
        return self.client['Product_Order'].placeOrder(order_container)


if __name__ == '__main__':
    order_size = 12000  # in GBs
    order_storage_type = 'file'  # 'block' or 'file'
    order_performance_type = 'tier'  # 'iops' or 'tier'
    """
    The performance_value below depends on the value of performance_type.
    If performance_type is 'iops':
        performance_value is the number of IOPS.
    If performance_type is 'tier:
        performance_value must be the tier level, which is a construct to represent a performance tier.
        The following are the valid tier levels in the format tier level => description
            - 100 => 0.25 IOPS per GB
            - 200 => 2 IOPS per GB
            - 300 => 4 IOPS per GB
            - 10000 => 10 IOPS per GB
        Examples:
            - For a volume by the IOPS, for 10000 IOPS the performance_value must be 10000
            - For a volume by the tier, for 2 IOPS per GB, the performance_value must be 200
    """
    order_performance_value = 200

    receipt = VolumeOrder().order(
        order_size,
        order_storage_type,
        order_performance_type,
        order_performance_value,
        'DALLAS09'
    )
    print receipt['orderId']
