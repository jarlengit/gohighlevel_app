from typing import Dict, List, Optional, Any
from highlevel import HighLevel
from highlevel.services.contacts.models import UpdateContactDto, UpsertContactDto
import frappe
import json
from deepdiff import DeepDiff
import asyncio

# ======================== 常量定义（统一硬编码值）========================
DOC_TYPE_CONTACTS = "GoHighLevel_contacts"  # 联系人文档类型
DOCTYPE_GHL_SET = "GoHighLevel_Set"         # GHL配置文档类型
FIELD_PRIVATE_TOKEN = "private_integration_token"  # 私域token字段名
# 事件类型常量
EVENT_TYPE_UPDATE = "GoHighLevel to Frappe Update"
EVENT_TYPE_CREATE = "GoHighLevel to Frappe Lead Created"
EVENT_TYPE_DELETE = "GoHighLevel to Frappe Delete"

# ======================== 通用工具函数（抽离重复逻辑）========================
def get_private_integration_token(location_id: str) -> Optional[str]:
    """
    通用方法：获取指定location_id对应的私域集成token
    :param location_id: GHL位置ID
    :return: 私域token | None
    """
    if not location_id:
        frappe.logger().warning("location_id为空，无法获取私域token")
        return None
    
    token = frappe.get_value(
        DOCTYPE_GHL_SET,
        {"check": 1, "name": location_id},
        FIELD_PRIVATE_TOKEN
    )
    if not token:
        frappe.logger().error(f"未找到有效的私域token，location_id={location_id}")
    return token

def get_hl_client(location_id: str) -> HighLevel:
    """
    获取 GoHighLevel 客户端实例（带严格参数校验）
    :param location_id: GHL位置ID
    :raises ValueError: 缺少token时抛出异常
    :return: GHL客户端实例
    """
    private_integration_token = get_private_integration_token(location_id)
    if not private_integration_token:
        error_msg = f"初始化GHL客户端失败：location_id={location_id} 无有效token"
        raise ValueError(error_msg)
    
    return HighLevel(private_integration_token=private_integration_token)

def validate_required_params(params: Dict[str, Any], required_keys: List[str]) -> bool:
    """
    通用方法：校验必填参数
    :param params: 待校验参数字典
    :param required_keys: 必填字段列表
    :return: 校验通过返回True，否则False
    """
    missing_keys = [key for key in required_keys if not params.get(key)]
    if missing_keys:
        frappe.logger().error(f"缺少必填参数：{missing_keys}")
        return False
    return True

# ======================== 数据转换函数（优化逻辑）========================
def gl_data_to_doc(ghl_contact_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    将GoHighLevel的联系人数据转换为Frappe文档格式（优化日期处理+类型转换）
    :param ghl_contact_data: GHL原始联系人数据
    :return: 适配Frappe的文档数据
    """
    doc_data = {"doctype": DOC_TYPE_CONTACTS}
    if not ghl_contact_data:
        return doc_data
    
    date_fields = ['dateUpdated', 'dateAdded', 'dateOfBirth']
    
    for key, value in ghl_contact_data.items():
        # 列表/字典转JSON字符串
        if isinstance(value, (list, dict)):
            doc_data[key] = json.dumps(value, ensure_ascii=False)
        # 日期字段格式化（兼容空值）
        elif key in date_fields and value:
            # 处理格式：2026-03-13T12:34:56.789Z -> 2026-03-13 12:34:56
            formatted_date = value.replace('T', ' ').replace('Z', '').split('.')[0]
            doc_data[key] = formatted_date
        # 其他字段直接赋值
        else:
            doc_data[key] = value
    
    # 确保name字段为GHL的contact_id
    if ghl_contact_data.get('id'):
        doc_data['name'] = ghl_contact_data['id']
    
    return doc_data

def doc_to_dict(doc_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    将Frappe文档数据转换为纯净字典（剔除系统字段）
    :param doc_data: Frappe文档原始字典
    :return: 过滤后的业务数据字典
    """
    # 系统字段列表（扩展更完整）
    system_fields = {
        'name', 'owner', 'creation', 'modified', 'modified_by', 'docstatus',
        'idx', 'parent', 'parenttype', 'parentfield', '_user_tags', '_comments',
        '_liked_by', '_assign', 'doctype'
    }
    return {k: v for k, v in doc_data.items() if k not in system_fields and v is not None}

# ======================== 异步工具函数（封装异步逻辑）========================
async def async_get_ghl_contact(hl_client: HighLevel, contact_id: str) -> Dict[str, Any]:
    """
    异步获取单个GHL联系人详情
    :param hl_client: GHL客户端实例
    :param contact_id: 联系人ID
    :return: 联系人详情字典
    """
    try:
        contact_resp = await hl_client.contacts.get_contact(contact_id=contact_id)
        return contact_resp.get('contact', {})
    except Exception as e:
        frappe.logger().error(f"异步获取GHL联系人失败，contact_id={contact_id}，错误：{str(e)}")
        return {}

async def async_get_ghl_contacts_batch(hl_client: HighLevel, location_id: str, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
    """
    分页获取GHL联系人（避免单次获取过多数据）
    :param hl_client: GHL客户端实例
    :param location_id: 位置ID
    :param limit: 每页条数（建议≤100，符合API最佳实践）
    :param offset: 偏移量
    :return: 联系人列表+元数据
    """
    try:
        return await hl_client.contacts.get_contacts(
            location_id=location_id,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        frappe.logger().error(f"分页获取GHL联系人失败，location_id={location_id}，offset={offset}，错误：{str(e)}")
        return {"contacts": [], "meta": {"total": 0}}

async def async_get_all_ghl_contacts(location_id: str, private_token: str) -> List[Dict[str, Any]]:
    """
    获取指定location_id下所有GHL联系人（自动分页）
    :param location_id: 位置ID
    :param private_token: 私域token
    :return: 所有联系人列表
    """
    all_contacts = []
    hl_client = HighLevel(private_integration_token=private_token)
    batch_size = 100
    offset = 0

    # 先获取总条数
    first_batch = await async_get_ghl_contacts_batch(hl_client, location_id, limit=1, offset=0)
    total = first_batch.get("meta", {}).get("total", 0)
    
    if total == 0:
        frappe.logger().info(f"location_id={location_id} 无联系人数据")
        return all_contacts

    # 分页获取所有数据
    while offset < total:
        batch_data = await async_get_ghl_contacts_batch(hl_client, location_id, batch_size, offset)
        batch_contacts = batch_data.get("contacts", [])
        all_contacts.extend(batch_contacts)
        offset += batch_size
        frappe.logger().debug(f"已获取location_id={location_id} 联系人 {len(all_contacts)}/{total} 条")

    return all_contacts

# ======================== 核心业务函数（优化逻辑+性能）========================
def upsert_contact(location_id: str, contact_id: str) -> Dict[str, Any]:
    """
    核心逻辑：更新/创建GHL联系人到Frappe
    :param location_id: GHL位置ID
    :param contact_id: GHL联系人ID
    :return: 处理结果字典
    """
    # 前置参数校验
    if not validate_required_params({"location_id": location_id, "contact_id": contact_id}, ["location_id", "contact_id"]):
        raise ValueError("location_id或contact_id为空，无法执行更新/创建操作")

    try:
        hl_client = get_hl_client(location_id)
        doc_exists = frappe.db.exists(DOC_TYPE_CONTACTS, contact_id)
        
        # 异步获取GHL最新数据
        ghl_contact_data = asyncio.run(async_get_ghl_contact(hl_client, contact_id))
        if not ghl_contact_data:
            raise ValueError(f"未获取到GHL联系人数据，contact_id={contact_id}")
        
        # 转换为Frappe文档格式
        doc_data = gl_data_to_doc(ghl_contact_data)
        
        res = {
            "event_type": "更新" if doc_exists else "创建",
            "docname": contact_id,
            "location_id": location_id,
            "raw_data": doc_data
        }

        if doc_exists:
            # 存在则更新（差异对比）
            doc = frappe.get_doc(DOC_TYPE_CONTACTS, contact_id)
            old_doc = doc_to_dict(doc.as_dict())
            doc.update(doc_data)
            doc.save(ignore_permissions=True)
            
            new_doc = doc_to_dict(doc.as_dict())
            diff = DeepDiff(old_doc, new_doc, ignore_order=True)
            
            res['diff'] = diff
            res['event_type'] = '无变化' if not diff else '更新'
        else:
            # 不存在则创建
            doc = frappe.new_doc(DOC_TYPE_CONTACTS)
            doc.update(doc_data)
            doc.insert(ignore_permissions=True)
        
        res['data'] = doc_to_dict(doc.as_dict())  # 只返回业务数据，减少数据量
        frappe.logger().info(f"联系人处理完成：{res['event_type']} - {contact_id}")
        return res

    except Exception as e:
        frappe.db.rollback()  # 异常回滚
        error_msg = f"处理联系人{contact_id}失败：{str(e)}"
        frappe.logger().error(error_msg)
        raise ValueError(error_msg)

def delete_contact(contact_id: str) -> Dict[str, Any]:
    """
    核心逻辑：删除Frappe中的GHL联系人
    :param contact_id: 联系人ID
    :return: 处理结果字典
    """
    res = {
        "event_type": "删除",
        "docname": contact_id,
        "status": "失败:文档不存在"
    }

    if frappe.db.exists(DOC_TYPE_CONTACTS, contact_id):
        doc = frappe.get_doc(DOC_TYPE_CONTACTS, contact_id)
        doc.delete(ignore_permissions=True)
        res['status'] = '成功'
        frappe.logger().info(f"联系人删除成功：{contact_id}")
    else:
        frappe.logger().warning(f"联系人删除失败：{contact_id} 不存在")
    
    return res

# ======================== 对外暴露的API（简化逻辑）========================
@frappe.whitelist(allow_guest=True)
def webhook_test():
    """WebHook测试接口：打印请求参数"""
    args = frappe._dict(frappe.request.json or frappe.form_dict)
    location_id = args.location.get('id') if args.location else None
    
    out = {
        "contact_id": args.contact_id,
        "event_type": args.workflow.get('name') if args.workflow else None,
        "location_id": location_id,
        "method": frappe.request.method,
        "private_integration_token": get_private_integration_token(location_id),
        "raw": args
    }
    frappe.logger().info(f"WebHook测试请求参数：{out}")
    return out

@frappe.whitelist(allow_guest=True)
def webhook_func():
    """GHL WebHook网关：处理创建/更新/删除事件"""
    try:
        args = frappe._dict(frappe.request.json or frappe.form_dict)
        
        # 基础参数提取+校验
        contact_id = args.contact_id
        event_type = args.workflow.get('name') if args.workflow else None
        location_id = args.location.get('id') if args.location else None
        method = frappe.request.method

        if not validate_required_params(
            {"contact_id": contact_id, "event_type": event_type, "location_id": location_id},
            ["contact_id", "event_type", "location_id"]
        ):
            frappe.throw("缺少WebHook必填参数：contact_id/event_type/location_id")

        frappe.logger().info(f"接收WebHook事件：type={event_type}, contact_id={contact_id}, method={method}")

        # 事件分发（优化为if-elif，避免逻辑冲突）
        if event_type in [EVENT_TYPE_UPDATE, EVENT_TYPE_CREATE] and method in ['POST', 'PUT']:
            res = upsert_contact(location_id, contact_id)
        elif event_type == EVENT_TYPE_DELETE and method in ['POST', 'DELETE']:
            res = delete_contact(contact_id)
            res['location_id'] = location_id
        else:
            res = {"status": "忽略", "reason": f"不支持的事件类型/请求方法：{event_type}/{method}"}
            frappe.logger().warning(f"忽略WebHook事件：{res['reason']}")

        # 统一返回格式
        frappe.response["message"] = f"处理GHL WebHook事件完成：{res.get('event_type', '忽略')} 联系人 {contact_id}"
        frappe.response["res"] = res
        frappe.db.commit()  # 统一提交，提升性能

    except Exception as e:
        frappe.db.rollback()
        error_msg = f"处理WebHook事件失败：{str(e)}"
        frappe.logger().error(error_msg)
        frappe.throw(error_msg)

@frappe.whitelist(allow_guest=True)
def data_up_task():
    """定时任务：全量同步GHL联系人到Frappe（优化分页+批量处理）"""
    try:
        # 获取所有启用的GHL配置
        ghl_configs = frappe.get_all(
            DOCTYPE_GHL_SET,
            fields=['name', 'locationid', FIELD_PRIVATE_TOKEN],
            filters={'check': 1}
        )
        if not ghl_configs:
            frappe.logger().info("无启用的GHL配置，同步任务终止")
            return {"账号数量": 0, "contacts": 0, "data": {}}

        # 初始化结果统计
        res = {
            '账号数量': len(ghl_configs),
            'contacts': 0,
            'data': {}
        }

        # 遍历每个配置同步联系人
        for config in ghl_configs:
            location_id = config.get('locationid')
            private_token = config.get(FIELD_PRIVATE_TOKEN)
            
            # 跳过无效配置
            if not location_id or not private_token:
                frappe.logger().warning(f"配置{config.get('name')}缺少locationid/token，跳过")
                continue

            # 异步获取该账号下所有联系人
            contacts = asyncio.run(async_get_all_ghl_contacts(location_id, private_token))
            res['contacts'] += len(contacts)

            # 批量处理联系人（减少数据库交互）
            for contact in contacts:
                contact_id = contact.get('id')
                if not contact_id:
                    frappe.logger().warning("跳过无ID的联系人：{contact}")
                    continue

                # 转换数据格式
                doc_data = gl_data_to_doc(contact)
                doc_key = f"{location_id}-{contact_id}"  # 唯一标识

                # 更新/创建逻辑
                if frappe.db.exists(DOC_TYPE_CONTACTS, contact_id):
                    doc = frappe.get_doc(DOC_TYPE_CONTACTS, contact_id)
                    old_doc = doc_to_dict(doc.as_dict())
                    doc.update(doc_data)
                    doc.save(ignore_permissions=True)
                    new_doc = doc_to_dict(doc.as_dict())
                    diff = DeepDiff(old_doc, new_doc, ignore_order=True)

                    res['data'][doc_key] = {
                        "status": "updated" if diff else 'pass',
                        "docname": contact_id,
                        "update_diff": diff
                    }
                else:
                    doc = frappe.new_doc(DOC_TYPE_CONTACTS)
                    doc.update(doc_data)
                    doc.insert(ignore_permissions=True)
                    res['data'][doc_key] = {
                        "status": "inserted",
                        "docname": contact_id,
                        "inserted_data": doc_to_dict(doc.as_dict())
                    }

        # 统一提交数据库事务（性能优化）
        frappe.db.commit()
        frappe.logger().info(f"GHL联系人同步完成：共处理{res['contacts']}条数据")
        return res

    except Exception as e:
        frappe.db.rollback()
        error_msg = f"同步GHL联系人失败：{str(e)}"
        frappe.logger().error(error_msg)
        frappe.throw(error_msg)