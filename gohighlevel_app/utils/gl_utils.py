
import frappe
import json
import asyncio
from contextlib import contextmanager
from typing import Dict, Any, Optional
from deepdiff import DeepDiff
from frappe.model.document import Document
from highlevel import HighLevel
from highlevel.services.contacts.models import UpdateContactDto, UpsertContactDto


# ======================== 常量定义 ========================
class ContactConstants:
    """联系人同步常量配置"""
    # 需要解析为列表的字段
    LIST_FIELDS = ["tags", "customfields"]
    # 需要解析为字典的字段
    DICT_FIELDS = ["dndsettings"]
    # 日志标题前缀
    LOG_TITLE = "GoHighLevel联系人同步"
    # 客户端初始化失败提示
    CLIENT_INIT_FAILED = "初始化GoHighLevel客户端失败"
fields = (
    "id", "locationId", "contactName", "firstName", "lastName", "firstNameRaw", "lastNameRaw", "companyName", "email", "phone",
    "dnd", "dndSettings", "type", "source", "assignedTo", "city", "state", "postalCode", "address1", "dateAdded", "dateUpdated",
    "dateOfBirth", "businessId", "tags",  "followers", "country", "website", "timezone", "profilePhoto", "additionalEmails",
    "customFields", "startAfter"
)
fields_map = frappe._dict({d:d.lower() for d in fields if d != d.lower()})
doc_fields_map = frappe._dict({d.lower():d for d in fields if d != d.lower()})

# ======================== 异步工具函数 ========================
@contextmanager
def reusable_async_loop():
    """可复用的异步循环上下文管理器（增加容错处理）"""
    loop = None
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        yield loop
    except Exception as e:
        frappe.logger().error(f"{ContactConstants.LOG_TITLE} - 异步循环创建失败: {str(e)}")
        raise
    finally:
        if loop:
            try:
                loop.close()
            except Exception as e:
                frappe.logger().error(f"{ContactConstants.LOG_TITLE} - 异步循环关闭失败: {str(e)}")
        asyncio.set_event_loop(None)


def get_highlevel_client(location_id) -> Optional[HighLevel]:
    """
    获取GoHighLevel客户端实例（优化版）
    :return: 初始化成功返回HighLevel实例，失败返回None
    """
    try:
        # 严格校验location_id
        if not location_id or not isinstance(location_id, str):
            frappe.logger().error(
                f"{ContactConstants.LOG_TITLE} - locationId无效: {location_id}"
            )
            return None

        # 获取私域token
        private_token = frappe.get_value(
            "GoHighLevel_Set",
            {"check": 1, "name": location_id},
            "private_integration_token"
        )

        if not private_token:
            frappe.logger().error(
                f"{ContactConstants.LOG_TITLE} - 未找到有效token:{private_token} locationId={location_id}"
            )
            return None

        return HighLevel(private_integration_token=private_token)

    except Exception as e:
        error_msg = f"{ContactConstants.LOG_TITLE} - 客户端初始化异常: {str(e)}, locationId={locationid}"
        frappe.logger().error(error_msg)
        frappe.throw(error_msg)