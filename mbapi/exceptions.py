class MBApiError(Exception):
    pass


class MBApiRequestError(MBApiError):
    """请求失败"""
    pass


class ProductNoExistError(MBApiError):
    pass


class ProductMultiError(MBApiError):
    pass


class OrderNotExistError(MBApiError):
    pass


class LoginError(MBApiError):
    """登录失败"""
    pass


class NotMergedOrderError(MBApiError):
    '''非合并订单错误'''
    pass


class MBApiBizError(MBApiError):
    """业务错误"""
    pass


