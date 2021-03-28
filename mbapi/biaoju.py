"""
镖局接口
"""
from lxml import html

from .base import MBApiBase
from .constant import (
    BIAOJU_API
)
from .config import COMMON_SHIPPING_FEE_ID, SPECIAL_SHIPPING_FEE_ID
from .exceptions import CalculateShippingFeeError


class BiaoJuApi(MBApiBase):
    def get_shipping_fee(self, shipping_fee_id, weight, country="US", postal_code=""):
        """获取单个国家物流价格
        :param shipping_fee_id: 物流自定义费用id
        :param weight: 重量
        :param country: 国家
        :param postal_code: 邮政编码
        """
        api = BIAOJU_API
        params = {
            "m": "customshippingfee",
            "a": "doCalculate",
        }
        data = {
            "data": "||".join([country, str(weight), str(postal_code)]),
            "ruleId": shipping_fee_id,
            "type": 1,
        }
        r_data = self.request('post', api, data=data, params=params)
        tree = html.fromstring(r_data["calculationRetHtml"])
        try:
            return float(tree.xpath("//tr[1]/td[4]/span[1]/text()")[0])
        except IndexError:
            log = (
                f"计算物流出错，请核对马帮物流自定义费用设置。"
                f"物流费用规则ID: {shipping_fee_id}, 重量: {weight}, 国家: {country}"
            )
            raise CalculateShippingFeeError(log)
