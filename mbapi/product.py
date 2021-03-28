import re
from dataclasses import dataclass, Field

from lxml import html

from .base import MBApiBase
from .constant import (
    MB_API,
    AAMZ_API,
)
from .exceptions import (
    ProductNoExistError,
    ProductMultiError,
)


class SpecialAttr:
    """特殊属性"""
    TRUE = "1"
    FALSE = "2"
    # 化妆品特殊属性
    ## 非以下三种
    NO_LIQUID_COSMETIC_FALSE = "0"
    ## 非液体(膏体)化妆品
    NO_LIQUID_COSMETIC = "1"
    ## 液体化妆品
    LIQUID_COSMETIC = "2"
    ## 液体非化妆品
    LIQUID_NO_COSMETIC = "3"


class ProductSearchOperate():
    EQUAL = "="
    LIKE_START = "like_start"
    LIKE = "like"
    LIKE_END = "like_end"


class StockProductSearchOperate():
    EQUAL = "="
    LIKE_START = "likeStart"
    LIKE = "like"
    LIKE_END = "likeEnd"


class StockProductSearchKey():
    STOCK_SKU = "Stock_stockSku"
    VIRTUAL_SKU = "StockVirtualSku_virtualSku"


class ComboProductSearchOperate():
    EQUAL = "="
    LIKE_START = "LikeStart"
    LIKE = "Like"
    LIKE_END = "LikeEnd"


class ComboProductSearchKey():
    COMBO_SKU = "comboSku"
    VIRTUAL_SKU = "virtualSku"


class ProductSearchType():
    """商品搜索类型: 库存SKU, 组合SKU"""
    STOCK_SKU_TYPE = "stock_sku"
    COMBO_SKU_TYPE = "combo_sku"


@dataclass
class ProductForShippingFee():
    """
    :param sku:
    :param cost:
    :param weight:
    :param is_battery: 带电
    :param is_tort: 侵权
    :param is_magnetic: 带磁
    :param is_no_liquid_cosmetic: 非液体化妆品
    :param is_liquid_cosmetic: 液体化妆品
    :param is_liquid_no_cosmetic: 液体非化妆品
    :param is_powder: 粉末
    """
    sku: str
    cost: float
    weight: float
    is_battery: bool
    is_tort: bool
    is_magnetic: bool
    is_no_liquid_cosmetic: bool
    is_liquid_cosmetic: bool
    is_liquid_no_cosmetic: bool
    is_powder: bool


@dataclass
class Product():
    """
    :param sku:
    :param cost:
    :param weight:
    :param is_battery: 带电
    :param is_tort: 侵权
    :param is_magnetic: 带磁
    :param is_no_liquid_cosmetic: 非液体化妆品
    :param is_liquid_cosmetic: 液体化妆品
    :param is_liquid_no_cosmetic: 液体非化妆品
    :param is_powder: 粉末
    """
    sku: str
    cost: float = 0
    weight: float = 0
    stock: int = 0
    unsent: int = 0
    purchasing: int = 0
    img_url: str = ""
    chinese: str = ""
    is_battery: bool = False
    is_tort: bool = False
    is_magnetic: bool = False
    is_no_liquid_cosmetic: bool = False
    is_liquid_cosmetic: bool = False
    is_liquid_no_cosmetic: bool = False
    is_powder: bool = False
    _ori_data: dict = Field(default_factory=dict)

    @classmethod
    def from_api(cls, stock_data):
        product = cls(stock_data["stockSku"])
        product.cost = float(stock_data['stockWarehouseData'][0]['stockCost'])
        product.weight = float(stock_data['weight'])
        product.stock = int(stock_data['stockQuantity'])
        product.img_url = stock_data['stockPicture']
        product.chinese = stock_data['declareName']
        product.is_battery = (stock_data['hasBattery'] == SpecialAttr.TRUE)
        product.is_tort= (stock_data['hasBattery'] == SpecialAttr.TRUE)
        product.is_magnetic = (stock_data['magnetic'] == SpecialAttr.TRUE)
        prodoct.is_no_liquid_cosmetic = (stock_data["noLiquidCosmetic"] == SpecialAttr.NO_LIQUID_COSMETIC)
        prodoct.is_liquid_cosmetic = (stock_data["noLiquidCosmetic"] == SpecialAttr.LIQUID_COSMETIC)
        prodoct.is_liquid_no_cosmetic = (stock_data["noLiquidCosmetic"] == SpecialAttr.LIQUID_NO_COSMETIC)
        prodoct.is_powder= (stock_data["powder"] == SpecialAttr.TRUE)
        product._ori_data = stock_data
        return product

    @classmethod
    def from_html_tree(cls, html_tree):
        sku = html_tree.xpath("./td[3]/p/a/text()")[0]
        product = Product(sku)
        product.cost = float(html_tree.xpath("./td[6]/text()")[0])
        product.weight = float(html_tree.xpath("./td[8]/text()")[0])
        # TODO: 组合SKU默认为特货
        product.is_battery = True
        return product


class ProductApi(MBApiBase):
    def get_stock_sku_info_list(
        self, search_key: StockProductSearchKey,
        search_content: str, operate: ProductSearchOperate
    ) -> list:
        '''获取库存SKU商品数据
        :param search_key: 查询方式
        :param content: 查询内容，目前用于sku搜索
        :return: 返回产品列表
        '''
        api = AAMZ_API
        params = {
            "mod": "stock.getStockList"
        }
        search_key_map = {
            'Stock_stockSku': '库存sku编号',
            'StockVirtualSku_virtualSku': '虚拟sku编号'
        }
        data = {
            'searchKey': search_key,
            'search-content': search_key_map[search_key],
            'searchValue': search_content,
            'operate': operate,
            'status': 3,
        }
        r_data = self.request('post', api, data=data, params=params)
        stock_data_list = r_data.get('stockData', [])
        return [Product.from_api(stock_data) for stock_data in stock_data_list]

    def get_combo_sku_info_list(
        self, search_key: StockProductSearchKey,
        search_content: str, operate: ProductSearchOperate
    ) -> list:
        '''获取库存SKU商品数据
        :param search_key: 查询方式
        :param content: 查询内容，目前用于sku搜索
        :return: 返回产品列表
        '''
        api = AAMZ_API
        params = {
            "mod": "combosku.getCombosSkuList"
        }
        data = {
            'searchLike': search_key,
            'searchKeywords': search_content,
            'operate': operate,
        }
        r_data = self.request('post', api, data=data, params=params)
        tree = html.fromstring(r_data["message"])
        ret_list = []
        for p_tree in tree.xpath("//tr/td[3]/p/a/../../.."):
            ret_list.append(Product.from_html_tree(p_tree))
        return ret_list

    def get_product_info(
        self, search_key,
        search_content: str, operate: ProductSearchOperate,
        error=True,
        search_type: ProductSearchType = ProductSearchType.STOCK_SKU_TYPE,
    ) -> list:
        """查询产品信息
        :param search_key: StockProductSearchKey 或 ComboProductSearchKey, 根据search_type进行输入
        """
        if search_type == ProductSearchType.STOCK_SKU_TYPE:
            operate = self._get_search_operater(search_type, operate)
            product_list = self.get_stock_sku_info_list(search_key, search_content, operate)
        elif search_type == ProductSearchType.COMBO_SKU_TYPE:
            operate = self._get_search_operater(search_type, operate)
            product_list = self.get_combo_sku_info_list(search_key, search_content, operate)
        else:
            raise ValueError(f"search_type: {search_type} 错误")

        if len(product_list) == 0:
            if error:
                raise ProductNoExistError('key:[%s] content:[%s] 查寻不到结果!' % (search_key, search_content))
            else:
                return Product(None)

        elif len(product_list) > 1:
            main_sku = self.get_main_sku(product_list[0].sku)
            # 如果匹配出来的结果不是属于同种商品
            if not all((product.sku.startswith(main_sku) for product in product_list[1:])):
                if error:
                    raise ProductMultiError('key:[%s] content:[%s] 查寻得到多种商品!' % (search_key, search_content))
                else:
                    return Product(None)
        return product_list[0]

    @staticmethod
    def get_main_sku(sku):
        '''获取主sku, 即子sku前缀. 如: TT0183F -> TT0183'''
        return re.match(r'^\D+\d+', sku).group()

    def get_product_info_from_stock_sku(self, sku, operate=ProductSearchOperate.LIKE_START, error=True):
        if sku.startswith("ZH"):
            search_type = ProductSearchType.COMBO_SKU_TYPE
            search_key = ComboProductSearchKey.COMBO_SKU
        else:
            search_type = ProductSearchType.STOCK_SKU_TYPE
            search_key = StockProductSearchKey.STOCK_SKU
        return self.get_product_info(search_key, sku, operate, error=error, search_type=search_type)

    def get_product_info_from_virtual_sku(self, sku, operate=ProductSearchOperate.LIKE_START, error=True):
        search_type = ProductSearchType.STOCK_SKU_TYPE
        search_key = StockProductSearchKey.VIRTUAL_SKU
        return self.get_product_info(search_key, sku, operate, error=error, search_type=search_type)

    def _get_search_operater(self, search_type: ProductSearchType, operate: ProductSearchOperate):
        stock_type_map = {
            ProductSearchOperate.EQUAL: StockProductSearchOperate.EQUAL,
            ProductSearchOperate.LIKE: StockProductSearchOperate.LIKE,
            ProductSearchOperate.LIKE_START: StockProductSearchOperate.LIKE_START,
            ProductSearchOperate.LIKE_END: StockProductSearchOperate.LIKE_END,
        }
        combo_type_map = {
            ProductSearchOperate.EQUAL: ComboProductSearchOperate.EQUAL,
            ProductSearchOperate.LIKE: ComboProductSearchOperate.LIKE,
            ProductSearchOperate.LIKE_START: ComboProductSearchOperate.LIKE_START,
            ProductSearchOperate.LIKE_END: ComboProductSearchOperate.LIKE_END,
        }
        type_operate_map = {
            ProductSearchType.STOCK_SKU_TYPE: stock_type_map,
            ProductSearchType.COMBO_SKU_TYPE: combo_type_map,
        }
        return type_operate_map[search_type][operate]
