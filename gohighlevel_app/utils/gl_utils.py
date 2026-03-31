
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



def get_contact_doc(data: Dict[str, Any]) -> Dict[str, Any]:
    """根据字段定义解析联系人数据
    {'id': 'Dg4BsoqIKLirUfQivfV2', 'locationId': 'Rft5PXFLw9e6vhedtZ86', 
    'contactName': '(example) jordan smith', 
    'firstName': '(example) jordan', 
    'lastName': 'smith', 
    'firstNameRaw': '(example) jordan', 
    'lastNameRaw': 'smith', 
    'companyName': "(Example) MacLaren's Pub", 
    'email': 'jordan.smith@example.com', 
    'phone': None, 'dnd': True, 
    'dndSettings': {'Call': {'status': 'inactive', 'code': '103'}, 'Email': {'status': 'inactive', 'code': '103'}, 
    'SMS': {'status': 'inactive', 'code': '103'}}, 
    'type': 'customer', 
    'source': None, 
    'assignedTo': None, 
    'city': None, 
    'state': None, 
    'postalCode': None, 
    'address1': None, 
    'dateAdded': '2026-03-05T14:20:36.702Z', 
    'dateUpdated': '2026-03-20T16:49:03.195Z', 
    'dateOfBirth': None, 
    'businessId': '69a9912c5ab8294414041a1a', 
    'tags': ['follow-up'], 
    'followers': [], 
    'country': 'US', 
    'website': None, 'timezone': None,
      'profilePhoto': None, 
      'additionalEmails': [{'validEmailDate': None, 'email': 
'jordan.office@example.com'}, 
{'validEmailDate': None, 'email': 'jordan@corporate.net'}, 
{'validEmailDate': None, 'email': 'jsmith@email.com'}], 
'customFields': [], 'startAfter': [1772720436702, 'Dg4BsoqIKLirUfQivfV2']}

    """
    frappe.logger().error(f"gl数据解析:{data}")  #记录日志

    
    doc = {'doctype': 'Contact'}
    key_dict = {
        'contactName':'middle_name',
        'firstName':'first_name',
        'lastName':'last_name',
        'companyName':'company_name',
        'email':'email_id',
        'phone':'phone',
        'type':'contact_type',
        'tags':'tags',
        'dateOfBirth':'date_of_birth',
        'website':'website',
        'additionalEmails':'email_ids',
        'followers':'followers',
        'profilePhoto':'image'    #头像
    }

    #关键字段不为空才进行赋值
    doc.update({key_dict.get(k,k):v for k,v in data.items() if k in key_dict and v is not None})
    
    doc['custom_gohighlevel_contact_id'] = data.get('id')
    if data.get('email'):
        doc['email_id'] = data.get('email')
        doc['email_ids'] = []
        doc['email_ids'].append({'email_id':data.get('email'), 'is_primary':1})
        
        for i in data.get('additionalEmails',[]):
            doc['email_ids'].append({'email_id':i.get('email'), 'is_primary':0})
    if data.get('phone'):
        doc['phone_nos'] = [{'phone': data.get('phone'),'is_primary_phone':True}]
     
    frappe.logger().error(f"gl数据解析2:{doc}")  #记录日志
    return doc


def get_dddress_doc(data: Dict[str, Any]) -> Dict[str, Any]:
    """根据字段定义解析联系人数据"""
    doc = {'doctype': 'Address'}

    key_dict = {
        'address1':'address_line1',
        'city':'city',
        'county':'county',
        'state':'state',
        'postalCode':'pincode',
        'country':'country',
        'email':'email_id',
        'phone':'phone',
        #'website':'website',
        #'locationId':'location_id'
        
    }

    #关键字段不为空才进行赋值
    doc.update({key_dict.get(k,k):v for k,v in data.items() if k in key_dict and v is not None})
    
    doc['custom_box'] = 1
    
    doc['address_title'] = doc.get('address_line1') if doc.get('address_line1') else '地址信息'
    
    if doc.get('city') is  None:
        doc['city'] = '*'
    
    return doc

def get_contact_lst(location_id: str) -> Optional[Dict[str, Any]]:
    """根据location_id和contact_id获取联系人数据"""
    ghc = get_highlevel_client(location_id)
    if not ghc:
        return None

    try:
        with reusable_async_loop() as loop:
            contacts_meta = loop.run_until_complete( ghc.contacts.get_contacts(location_id=location_id,limit=1))
            total = contacts_meta.get("meta", {}).get('total', 0)
            if total == 0:
                frappe.msgprint(f"记录 {ght.get('name')} 无联系人数据")
                return
            
            contacts_data = loop.run_until_complete( ghc.contacts.get_contacts(location_id=location_id,limit=total))
            contacts = contacts_data. get('contacts', [])
            return contacts

    except Exception as e:
        frappe.logger().error(f"{ContactConstants.LOG_TITLE} - 获取联系人数据异常: {str(e)}, locationId={location_id}, {data}")
        return None

def upinsert_contact_doc(data: Dict[str, Any]) -> Dict[str, Any]:
    """根据字段定义解析联系人数据"""
    doc = get_contact_doc(data)             #提取联系人文档数据
    address_doc = get_dddress_doc(data)     #提取地址文档数据
    frappe.logger().error(f"gl数据解析完成1:{data}")  #记录日志

    frappe.logger().error(f"gl数据解析完成2:{address_doc} \n\n{doc}")  #记录日志
    #地址数据不为空才进行创建
    if address_doc.get("address_line1"):
        address_doc['locationid'] = data.get('locationId')
        #更新国家代码,提取文档name
        address_doc['country'] = frappe.db.exists('Country',{'code': address_doc.get('country')})
        #frappe.get_value('Country',  filters={'code': address_doc.get('country')},fieldname='name')
        frappe.logger().error(f"记录地址数据:{address_doc}")  # 记录错误日志

        #处理地址数据，先判断是否存在相同locationid和address_line1的地址记录，如果存在则更新，不存在则创建
        if address :=frappe.db.exists(address_doc['doctype'], {'locationid': data.get('locationId'), 'address_line1': address_doc.get('address_line1')}):
            #存在
            address_doc.pop("name", None)
            address_doc = frappe.get_doc(address_doc['doctype'], address).update(address_doc).save(ignore_permissions=True)
        else:
            #不存在
            address_doc = frappe.new_doc(address_doc['doctype']).update(address_doc).insert(ignore_permissions=True)
        frappe.db.commit()
        doc['address'] = address_doc.name

    #更新联系人数据，先判断是否存在相同locationid和name的联系人记录，如果存在则更新，不存在则创建
    if ct :=frappe.db.exists('Contact', {'custom_gohighlevel_contact_id': doc.get('custom_gohighlevel_contact_id')}):
        #存在
        doc.pop("name", None)
        doc = frappe.get_doc(doc['doctype'], ct).update(doc).save(ignore_permissions=True)
    else:
        #不存在
        doc = frappe.new_doc(doc['doctype']).update(doc).insert(ignore_permissions=True,ignore_if_duplicate=True)
    frappe.logger().info(f"new_doc数据:{doc.as_dict()}")  #记录
    frappe.db.commit()

    return doc



